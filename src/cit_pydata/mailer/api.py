import base64
import os
import re

from cit_pydata.util import api as util_api
from cit_pydata.aws import api as aws_api

EMAIL_PATTERN = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
FILE_ALIAS_PATTERN = re.compile(r"^[a-zA-Z0-9_-][a-zA-Z0-9_. -]*[a-zA-Z0-9_-]$")

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
# Attachments whose combined size is at or below this threshold are sent inline
# (base64) in a single sendMail call. Above it, the message is sent via a draft
# plus an upload session. sendMail's total request limit is ~4 MB, so 3 MB is a
# safe cutover point.
MAX_INLINE_ATTACHMENT_BYTES = 3 * 1024 * 1024
# Absolute per-attachment ceiling. Graph upload sessions support up to 150 MB.
MAX_ATTACHMENT_BYTES = 150 * 1024 * 1024
# Chunk size for upload sessions. Every chunk except the last must be a multiple
# of 320 KiB per the Graph API.
UPLOAD_CHUNK_SIZE = 12 * 320 * 1024  # 3,932,160 bytes (~3.75 MiB)


class MailClient:
    """
    Sends email via the Microsoft Graph API.

    Small messages (attachments totaling <= ~3 MB) are sent in a single sendMail
    call. Larger messages automatically switch to a draft + chunked upload
    session, which supports individual attachments up to 150 MB (e.g. Salesforce
    Files/ContentVersion PDFs).

    Authenticates with the OAuth2 client-credentials flow using an app
    registration (tenant_id / client_id / client_secret) that has been granted
    the application permission "Mail.Send". Those credentials are read from AWS
    SSM Parameter Store as a JSON document, e.g.:

        {
            "tenant_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
            "client_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
            "client_secret": "xxxxxxxxxxxxxxxxxxxxxxxx"
        }

    A single MailClient can be reused to send multiple emails; the access token
    is fetched lazily on first send and reused.
    """

    def __init__(
        self,
        ssm_parameter_name: str = "/eab-pydata/graph/config",
        environment: str = None,
        iam_user: str = None,
        approved_senders: list = None,
        logger=None,
    ):
        self.logger = util_api.get_logger(__name__, "INFO") if not logger else logger

        self.ssm_parameter_name = ssm_parameter_name

        # Only needed for local (non-AWS) auth; on AWS the execution role is used.
        self.aws_environment = environment or util_api.get_environment_variable(
            logger=self.logger, variable_name="aws_auth_environment"
        )
        self.aws_iam_user = iam_user or util_api.get_environment_variable(
            logger=self.logger, variable_name="aws_auth_iam_user"
        )

        # When set, sender_email must match one of these (case-insensitive). None skips the check.
        self.approved_senders = (
            [s.lower() for s in approved_senders] if approved_senders else None
        )

        # Fetched lazily on first send.
        self._access_token = None

    @staticmethod
    def is_valid_email(email: str) -> bool:
        return EMAIL_PATTERN.match(email) is not None

    @staticmethod
    def is_valid_file_alias(alias: str) -> bool:
        return FILE_ALIAS_PATTERN.match(alias) is not None

    def _validate_email_list(self, email_list: list):
        invalid = [e for e in email_list if not self.is_valid_email(e.strip())]
        if invalid:
            raise ValueError(
                f"The following email addresses are invalid: {', '.join(invalid)}"
            )

    def _check_sender(self, sender_email: str):
        if not self.is_valid_email(sender_email):
            raise ValueError(f'Sender email "{sender_email}" is not a valid email')
        if (
            self.approved_senders is not None
            and sender_email.lower() not in self.approved_senders
        ):
            raise ValueError(
                f'Sender email "{sender_email}" is not in the approved sender list'
            )

    def _get_graph_config(self) -> dict:
        """Reads the Graph app-registration credentials JSON from SSM."""
        import json

        ssm_client = aws_api.SSMClient(
            environment=self.aws_environment,
            iam_user=self.aws_iam_user,
            logger=self.logger,
        )
        raw = ssm_client.get_parameter(
            name=self.ssm_parameter_name, with_decryption=True
        )
        if not raw:
            raise ValueError(
                f'Graph config not found in SSM at "{self.ssm_parameter_name}"'
            )
        config = json.loads(raw)
        for key in ("tenant_id", "client_id", "client_secret"):
            if not config.get(key):
                raise ValueError(f'Graph config is missing required key "{key}"')
        return config

    def _authenticate(self):
        """Fetches and caches an OAuth2 access token via the client-credentials flow."""
        import requests

        if self._access_token is not None:
            return

        config = self._get_graph_config()
        token_url = (
            f"https://login.microsoftonline.com/{config['tenant_id']}"
            "/oauth2/v2.0/token"
        )
        payload = {
            "grant_type": "client_credentials",
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "scope": "https://graph.microsoft.com/.default",
        }

        response = requests.post(token_url, data=payload)
        try:
            response.raise_for_status()
        except Exception as e:
            self.logger.error(f"Failed to authenticate to Microsoft Graph: {e}")
            self.logger.error(response.text)
            raise

        self._access_token = response.json().get("access_token")
        if not self._access_token:
            raise ValueError("Microsoft Graph token response did not include an access_token")

    def _read_attachment(self, attachment, attachment_alias: str = None):
        """
        Reads an attachment and returns a (filename, content_bytes) tuple.

        `attachment` is a filepath (str) or an object exposing to_csv() (e.g. a
        pandas DataFrame). `attachment_alias` renames the attachment to something
        more recipient-friendly (e.g. "Contract_Details").
        """
        if attachment_alias and not self.is_valid_file_alias(attachment_alias):
            raise ValueError(
                f'Attachment alias "{attachment_alias}" uses an unexpected format'
            )

        if isinstance(attachment, str):
            if not os.path.exists(attachment):
                raise ValueError(
                    f'Attachment file could not be found at path "{attachment}"'
                )
            filename = (
                attachment_alias + os.path.splitext(attachment)[1]
                if attachment_alias
                else os.path.basename(attachment)
            )
            with open(attachment, "rb") as file_obj:
                content_bytes = file_obj.read()
        elif hasattr(attachment, "to_csv"):
            filename = f"{attachment_alias}.csv" if attachment_alias else "attachment.csv"
            content_bytes = attachment.to_csv(index=False).encode("utf-8")
        else:
            raise ValueError(
                "attachment must be a filepath (str) or an object with a to_csv() "
                "method (e.g. a pandas DataFrame)"
            )

        if len(content_bytes) > MAX_ATTACHMENT_BYTES:
            raise ValueError(
                f'Attachment "{filename}" ({len(content_bytes)} bytes) exceeds the '
                f"{MAX_ATTACHMENT_BYTES} byte Graph attachment limit"
            )

        return filename, content_bytes

    def _normalize_attachments(self, attachment, attachment_alias, attachments):
        """
        Collapses the single-attachment args and the optional `attachments` list
        into one list of (filename, content_bytes) tuples.

        Each element of `attachments` is either:
          - a filepath (str) or DataFrame-like object, or
          - a dict {"attachment": <filepath|DataFrame>, "alias": <str>}.
        """
        specs = []
        if attachment is not None:
            specs.append(self._read_attachment(attachment, attachment_alias))
        for item in attachments or []:
            if isinstance(item, dict):
                specs.append(
                    self._read_attachment(item.get("attachment"), item.get("alias"))
                )
            else:
                specs.append(self._read_attachment(item))
        return specs

    @staticmethod
    def _build_inline_attachment(filename: str, content_bytes: bytes) -> dict:
        """Builds a Graph inline fileAttachment dict (base64-encoded content)."""
        return {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": filename,
            "contentBytes": base64.b64encode(content_bytes).decode("ascii"),
        }

    @staticmethod
    def _to_recipient_list(emails: str) -> list:
        return [
            {"emailAddress": {"address": e.strip()}}
            for e in emails.split(",")
            if e.strip()
        ]

    def _auth_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }

    def send_email(
        self,
        sender_email: str,
        receiver_email: str,
        subject: str,
        body: str = None,
        cc_email: str = None,
        attachment=None,
        attachment_alias: str = None,
        attachments: list = None,
        body_type: str = "HTML",
        save_to_sent_items: bool = False,
    ) -> bool:
        """
        Sends an email via Microsoft Graph. Returns True on success, False on failure.

        sender_email must be a mailbox the app registration is authorized to send
        as. receiver_email/cc_email accept comma-separated addresses.
        body_type is "HTML" or "Text".

        Attachments: pass a single one via `attachment`/`attachment_alias`, and/or
        several via `attachments` (a list of filepaths/DataFrames or dicts
        {"attachment": ..., "alias": ...}). Each attachment is a filepath (str) or
        an object with a to_csv() method (e.g. a pandas DataFrame).

        Attachments totaling <= ~3 MB are sent inline in a single sendMail call.
        Larger payloads automatically use a draft + chunked upload session so
        individual attachments up to 150 MB are supported. Note: the upload-session
        path always saves the message to the sender's Sent Items, so
        save_to_sent_items is only honored on the inline sendMail path.
        """
        if body_type not in ("HTML", "Text"):
            self.logger.error('body_type must be "HTML" or "Text"')
            return False

        try:
            self._check_sender(sender_email)
            self._validate_email_list(receiver_email.split(","))
            if cc_email:
                self._validate_email_list(cc_email.split(","))
            if not subject:
                raise ValueError("subject cannot be empty")
            attachment_specs = self._normalize_attachments(
                attachment, attachment_alias, attachments
            )
        except ValueError as e:
            self.logger.error(e)
            return False

        message = {
            "subject": subject,
            "body": {
                "contentType": body_type,
                "content": body or "-No message was included by the sender-",
            },
            "toRecipients": self._to_recipient_list(receiver_email),
        }
        if cc_email:
            message["ccRecipients"] = self._to_recipient_list(cc_email)

        try:
            self._authenticate()
        except Exception:
            return False

        total_bytes = sum(len(content) for _, content in attachment_specs)
        attachment_names = ", ".join(name for name, _ in attachment_specs) or None
        self.logger.info(
            f"Preparing to send email via Graph: subject={subject}, sender={sender_email}, "
            f"receiver={receiver_email}, cc={cc_email}, attachments={attachment_names}, "
            f"total_attachment_bytes={total_bytes}"
        )

        if total_bytes <= MAX_INLINE_ATTACHMENT_BYTES:
            return self._send_via_send_mail(
                sender_email, message, attachment_specs, save_to_sent_items
            )
        return self._send_via_upload_session(sender_email, message, attachment_specs)

    def _send_via_send_mail(
        self, sender_email, message, attachment_specs, save_to_sent_items
    ) -> bool:
        """Sends a message in a single sendMail call with inline attachments."""
        import requests

        if attachment_specs:
            message = dict(message)
            message["attachments"] = [
                self._build_inline_attachment(name, content)
                for name, content in attachment_specs
            ]

        send_mail_url = f"{GRAPH_BASE_URL}/users/{sender_email}/sendMail"
        payload = {"message": message, "saveToSentItems": save_to_sent_items}

        response = None
        try:
            response = requests.post(
                send_mail_url, headers=self._auth_headers(), json=payload
            )
            response.raise_for_status()
            self.logger.info("Email sent successfully (sendMail)")
            return True
        except Exception as e:
            self.logger.error(f"Error occurred while sending the email: {e}")
            if response is not None and getattr(response, "text", None):
                self.logger.error(response.text)
            return False

    def _send_via_upload_session(self, sender_email, message, attachment_specs) -> bool:
        """
        Sends a message whose attachments are too large for a single sendMail
        request. Creates a draft, attaches each file (inline for small, chunked
        upload session for large), then sends the draft.
        """
        import requests

        headers = self._auth_headers()

        # 1. Create the draft message (without attachments).
        create_url = f"{GRAPH_BASE_URL}/users/{sender_email}/messages"
        response = None
        try:
            response = requests.post(create_url, headers=headers, json=message)
            response.raise_for_status()
            message_id = response.json()["id"]
        except Exception as e:
            self.logger.error(f"Failed to create Graph draft message: {e}")
            if response is not None and getattr(response, "text", None):
                self.logger.error(response.text)
            return False

        # 2. Attach each file to the draft.
        for filename, content_bytes in attachment_specs:
            try:
                if len(content_bytes) <= MAX_INLINE_ATTACHMENT_BYTES:
                    self._add_inline_attachment_to_draft(
                        sender_email, message_id, filename, content_bytes
                    )
                else:
                    self._upload_large_attachment(
                        sender_email, message_id, filename, content_bytes
                    )
            except Exception as e:
                self.logger.error(f'Failed to attach "{filename}" to draft: {e}')
                return False

        # 3. Send the draft.
        send_url = f"{GRAPH_BASE_URL}/users/{sender_email}/messages/{message_id}/send"
        response = None
        try:
            response = requests.post(send_url, headers=headers)
            response.raise_for_status()
            self.logger.info("Email sent successfully (upload session)")
            return True
        except Exception as e:
            self.logger.error(f"Failed to send Graph draft message: {e}")
            if response is not None and getattr(response, "text", None):
                self.logger.error(response.text)
            return False

    def _add_inline_attachment_to_draft(
        self, sender_email, message_id, filename, content_bytes
    ):
        """Adds a small (<= 3 MB) attachment directly to an existing draft."""
        import requests

        url = (
            f"{GRAPH_BASE_URL}/users/{sender_email}/messages/{message_id}/attachments"
        )
        body = self._build_inline_attachment(filename, content_bytes)
        response = requests.post(url, headers=self._auth_headers(), json=body)
        response.raise_for_status()

    def _upload_large_attachment(
        self, sender_email, message_id, filename, content_bytes
    ):
        """Uploads a large (> 3 MB) attachment to a draft via an upload session."""
        import requests

        size = len(content_bytes)

        # Create the upload session.
        session_url = (
            f"{GRAPH_BASE_URL}/users/{sender_email}/messages/{message_id}"
            "/attachments/createUploadSession"
        )
        body = {
            "AttachmentItem": {
                "attachmentType": "file",
                "name": filename,
                "size": size,
            }
        }
        response = requests.post(session_url, headers=self._auth_headers(), json=body)
        response.raise_for_status()
        upload_url = response.json()["uploadUrl"]

        # Upload the content in chunks. Every chunk except the last must be a
        # multiple of 320 KiB. The upload URL is pre-authorized, so it takes no
        # Authorization header.
        start = 0
        while start < size:
            end = min(start + UPLOAD_CHUNK_SIZE, size)
            chunk = content_bytes[start:end]
            chunk_headers = {
                "Content-Length": str(end - start),
                "Content-Range": f"bytes {start}-{end - 1}/{size}",
            }
            response = requests.put(upload_url, headers=chunk_headers, data=chunk)
            response.raise_for_status()
            start = end
