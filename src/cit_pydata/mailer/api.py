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

# Default per-request timeout (seconds) applied to every Graph/token HTTP call so
# a hung connection can never block a job indefinitely. Override with the
# GRAPH_REQUEST_TIMEOUT_SECONDS environment variable.
DEFAULT_TIMEOUT_SECONDS = 60
# Refresh the cached access token this many seconds before it actually expires,
# so a long-lived MailClient never sends with an almost-expired token.
TOKEN_REFRESH_SAFETY_SECONDS = 120

# Senders allowed by default when the caller does not pass an explicit
# approved_senders list. Microsoft Graph application permissions can be broad;
# this guard prevents callers from accidentally sending as arbitrary mailboxes.
DEFAULT_APPROVED_SENDERS = [
    "CorpIT_DICommunications@eab.com",
    "sfadmin@eab.com",
]


class MailClient:
    """
    Sends email via the Microsoft Graph API.

    Small messages (attachments totaling <= ~3 MB) are sent in a single sendMail
    call. Larger messages automatically switch to a draft + chunked upload
    session, which supports individual attachments up to 150 MB (e.g. Salesforce
    Files/ContentVersion PDFs).

    Authenticates with the OAuth2 client-credentials flow using an app
    registration (tenant_id / client_id / client_secret) that has been granted
    the application permission "Mail.Send". Those credentials are read from three
    AWS SSM Parameter Store parameters under a common base path (the same
    convention as the CorpIT mailer). For base path
    "/eab-pydata/graphapp/python_mailer":

        /eab-pydata/graphapp/python_mailer/tenant_id       (String)
        /eab-pydata/graphapp/python_mailer/client_id       (String)
        /eab-pydata/graphapp/python_mailer/client_secret   (SecureString)

    Each credential can also be supplied via environment variable (checked before
    SSM) for local development or emergency override:
        tenant_id:     GRAPH_TENANT_ID / MS_GRAPH_TENANT_ID / AZURE_TENANT_ID / AAD_TENANT_ID
        client_id:     GRAPH_CLIENT_ID / MS_GRAPH_CLIENT_ID / AZURE_CLIENT_ID / AAD_CLIENT_ID
        client_secret: GRAPH_CLIENT_SECRET / MS_GRAPH_CLIENT_SECRET / AZURE_CLIENT_SECRET / AAD_CLIENT_SECRET

    A single MailClient can be reused to send multiple emails; the access token
    is fetched lazily on first send and refreshed automatically before it expires.
    """

    def __init__(
        self,
        ssm_parameter_name: str = "/eab-pydata/graphapp/python_mailer",
        environment: str = None,
        iam_user: str = None,
        approved_senders: list = None,
        logger=None,
    ):
        import requests

        self.logger = util_api.get_logger(__name__, "INFO") if not logger else logger

        # Base SSM path holding the tenant_id / client_id / client_secret params.
        self.ssm_parameter_name = ssm_parameter_name

        # Only needed for local (non-AWS) auth; on AWS the execution role is used.
        self.aws_environment = environment or util_api.get_environment_variable(
            logger=self.logger, variable_name="aws_auth_environment"
        )
        self.aws_iam_user = iam_user or util_api.get_environment_variable(
            logger=self.logger, variable_name="aws_auth_iam_user"
        )

        # sender_email must match one of these (case-insensitive). When the caller
        # does not supply a list, DEFAULT_APPROVED_SENDERS is enforced so a sender
        # whitelist is always applied.
        self.approved_senders = [
            s.lower() for s in (approved_senders or DEFAULT_APPROVED_SENDERS)
        ]

        # Per-request timeout and upload-retry budget (env-configurable).
        self.request_timeout_seconds = int(
            os.getenv("GRAPH_REQUEST_TIMEOUT_SECONDS", str(DEFAULT_TIMEOUT_SECONDS))
        )
        self.max_upload_retries = int(os.getenv("GRAPH_UPLOAD_MAX_RETRIES", "3"))

        # Reuse one Session for connection pooling / keep-alive across calls.
        self.session = requests.Session()

        # Fetched lazily on first send and refreshed before expiry.
        self._access_token = None
        self._access_token_expires_at = 0.0

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
        """Reads the Graph app-registration credentials.

        For each of tenant_id / client_id / client_secret the value is taken from
        an environment variable if set (local dev / emergency override), otherwise
        from SSM under the configured base path: {base}/tenant_id, {base}/client_id,
        {base}/client_secret. with_decryption=True is safe for both String and
        SecureString parameters. The SSM client is only built if at least one value
        must be read from SSM, so a fully env-configured caller needs no AWS auth.
        """
        env_names = {
            "tenant_id": (
                "GRAPH_TENANT_ID",
                "MS_GRAPH_TENANT_ID",
                "AZURE_TENANT_ID",
                "AAD_TENANT_ID",
            ),
            "client_id": (
                "GRAPH_CLIENT_ID",
                "MS_GRAPH_CLIENT_ID",
                "AZURE_CLIENT_ID",
                "AAD_CLIENT_ID",
            ),
            "client_secret": (
                "GRAPH_CLIENT_SECRET",
                "MS_GRAPH_CLIENT_SECRET",
                "AZURE_CLIENT_SECRET",
                "AAD_CLIENT_SECRET",
            ),
        }

        base = self.ssm_parameter_name.rstrip("/")
        config = {}
        ssm_client = None
        for key in ("tenant_id", "client_id", "client_secret"):
            # 1. Environment variable override.
            value = None
            for env_name in env_names[key]:
                value = os.getenv(env_name)
                if value:
                    self.logger.debug(
                        f"Loaded Graph credential '{key}' from environment variable '{env_name}'"
                    )
                    break

            # 2. Fall back to SSM.
            if not value:
                if ssm_client is None:
                    ssm_client = aws_api.SSMClient(
                        environment=self.aws_environment,
                        iam_user=self.aws_iam_user,
                        logger=self.logger,
                    )
                parameter_name = f"{base}/{key}"
                value = ssm_client.get_parameter(
                    name=parameter_name, with_decryption=True
                )
                if not value or not str(value).strip():
                    raise ValueError(
                        f'Graph config parameter not found or empty in SSM: "{parameter_name}"'
                    )

            config[key] = str(value).strip()
        return config

    def _authenticate(self):
        """Fetches and caches an OAuth2 access token via the client-credentials flow.

        The token is refreshed automatically once it is within
        TOKEN_REFRESH_SAFETY_SECONDS of expiry, so a reused MailClient never sends
        with a stale token.
        """
        import time

        now = time.time()
        if self._access_token is not None and now < (
            self._access_token_expires_at - TOKEN_REFRESH_SAFETY_SECONDS
        ):
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

        response = self.session.post(
            token_url, data=payload, timeout=self.request_timeout_seconds
        )
        try:
            response.raise_for_status()
        except Exception as e:
            self.logger.error(f"Failed to authenticate to Microsoft Graph: {e}")
            self.logger.error(response.text)
            raise

        token_payload = response.json()
        self._access_token = token_payload.get("access_token")
        if not self._access_token:
            raise ValueError(
                "Microsoft Graph token response did not include an access_token"
            )
        self._access_token_expires_at = now + int(
            token_payload.get("expires_in", 3599)
        )

    def _read_attachment(
        self, attachment, attachment_alias: str = None, compression: bool = False
    ):
        """
        Reads an attachment and returns a (filename, content_bytes) tuple.

        `attachment` is a filepath (str) or an object exposing to_csv() (e.g. a
        pandas DataFrame). `attachment_alias` renames the attachment to something
        more recipient-friendly (e.g. "Contract_Details"). When `compression` is
        True the content is gzip-compressed and ".gz" is appended to the filename.
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

        if compression:
            import gzip

            content_bytes = gzip.compress(content_bytes)
            filename = filename + ".gz"

        if len(content_bytes) > MAX_ATTACHMENT_BYTES:
            raise ValueError(
                f'Attachment "{filename}" ({len(content_bytes)} bytes) exceeds the '
                f"{MAX_ATTACHMENT_BYTES} byte Graph attachment limit"
            )

        return filename, content_bytes

    def _normalize_attachments(
        self, attachment, attachment_alias, attachments, compression: bool = False
    ):
        """
        Collapses the single-attachment args and the optional `attachments` list
        into one list of (filename, content_bytes) tuples.

        Each element of `attachments` is either:
          - a filepath (str) or DataFrame-like object, or
          - a dict {"attachment": <filepath|DataFrame>, "alias": <str>}.

        `compression` (when True) gzip-compresses every attachment.
        """
        specs = []
        if attachment is not None:
            specs.append(
                self._read_attachment(attachment, attachment_alias, compression)
            )
        for item in attachments or []:
            if isinstance(item, dict):
                specs.append(
                    self._read_attachment(
                        item.get("attachment"), item.get("alias"), compression
                    )
                )
            else:
                specs.append(self._read_attachment(item, None, compression))
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
        compression: bool = False,
    ) -> bool:
        """
        Sends an email via Microsoft Graph. Returns True on success, False on failure.

        sender_email must be a mailbox the app registration is authorized to send
        as. receiver_email/cc_email accept comma-separated addresses.
        body_type is "HTML" or "Text".

        Attachments: pass a single one via `attachment`/`attachment_alias`, and/or
        several via `attachments` (a list of filepaths/DataFrames or dicts
        {"attachment": ..., "alias": ...}). Each attachment is a filepath (str) or
        an object with a to_csv() method (e.g. a pandas DataFrame). When
        `compression` is True every attachment is gzip-compressed (".gz" appended).

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
                attachment, attachment_alias, attachments, compression
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
        except Exception as e:
            self.logger.error(f"Failed to authenticate to Microsoft Graph: {e}")
            return False

        total_bytes = sum(len(content) for _, content in attachment_specs)
        attachment_names = ", ".join(name for name, _ in attachment_specs) or None
        self.logger.info(
            f"Preparing to send email via Graph: subject={subject}, sender={sender_email}, "
            f"receiver={receiver_email}, cc={cc_email}, attachments={attachment_names}, "
            f"total_attachment_bytes={total_bytes}, compression={compression}"
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
            response = self.session.post(
                send_mail_url,
                headers=self._auth_headers(),
                json=payload,
                timeout=self.request_timeout_seconds,
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
        upload session for large), then sends the draft. If attaching or sending
        fails after the draft is created, the draft is deleted so failed jobs do
        not leave orphaned messages in Drafts.
        """
        headers = self._auth_headers()

        # 1. Create the draft message (without attachments).
        create_url = f"{GRAPH_BASE_URL}/users/{sender_email}/messages"
        response = None
        try:
            response = self.session.post(
                create_url,
                headers=headers,
                json=message,
                timeout=self.request_timeout_seconds,
            )
            response.raise_for_status()
            message_id = response.json()["id"]
        except Exception as e:
            self.logger.error(f"Failed to create Graph draft message: {e}")
            if response is not None and getattr(response, "text", None):
                self.logger.error(response.text)
            return False

        try:
            # 2. Attach each file to the draft.
            for filename, content_bytes in attachment_specs:
                if len(content_bytes) <= MAX_INLINE_ATTACHMENT_BYTES:
                    self._add_inline_attachment_to_draft(
                        sender_email, message_id, filename, content_bytes
                    )
                else:
                    self._upload_large_attachment(
                        sender_email, message_id, filename, content_bytes
                    )

            # 3. Send the draft.
            send_url = (
                f"{GRAPH_BASE_URL}/users/{sender_email}/messages/{message_id}/send"
            )
            response = self.session.post(
                send_url, headers=headers, timeout=self.request_timeout_seconds
            )
            response.raise_for_status()
            self.logger.info("Email sent successfully (upload session)")
            return True
        except Exception as e:
            self.logger.error(f"Failed to send Graph draft message: {e}")
            if response is not None and getattr(response, "text", None):
                self.logger.error(response.text)
            # Clean up the created-but-unsent draft.
            self._delete_draft(sender_email, message_id)
            return False

    def _delete_draft(self, sender_email, message_id):
        """Best-effort deletion of a draft left behind by a failed send."""
        try:
            delete_url = (
                f"{GRAPH_BASE_URL}/users/{sender_email}/messages/{message_id}"
            )
            self.session.delete(
                delete_url,
                headers=self._auth_headers(),
                timeout=self.request_timeout_seconds,
            )
        except Exception as cleanup_exc:
            self.logger.warning(
                f"Unable to delete failed draft message after Graph send failure: {cleanup_exc}"
            )

    def _add_inline_attachment_to_draft(
        self, sender_email, message_id, filename, content_bytes
    ):
        """Adds a small (<= 3 MB) attachment directly to an existing draft."""
        url = (
            f"{GRAPH_BASE_URL}/users/{sender_email}/messages/{message_id}/attachments"
        )
        body = self._build_inline_attachment(filename, content_bytes)
        response = self.session.post(
            url,
            headers=self._auth_headers(),
            json=body,
            timeout=self.request_timeout_seconds,
        )
        response.raise_for_status()

    def _upload_large_attachment(
        self, sender_email, message_id, filename, content_bytes
    ):
        """Uploads a large (> 3 MB) attachment to a draft via an upload session."""
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
        response = self.session.post(
            session_url,
            headers=self._auth_headers(),
            json=body,
            timeout=self.request_timeout_seconds,
        )
        response.raise_for_status()
        upload_url = response.json()["uploadUrl"]

        # Upload the content in chunks. Every chunk except the last must be a
        # multiple of 320 KiB. The upload URL is pre-authorized, so it takes no
        # Authorization header. Chunk PUTs are retried on transient errors.
        start = 0
        while start < size:
            end = min(start + UPLOAD_CHUNK_SIZE, size)
            chunk = content_bytes[start:end]
            chunk_headers = {
                "Content-Length": str(end - start),
                "Content-Range": f"bytes {start}-{end - 1}/{size}",
            }
            response = self._put_upload_chunk_with_retry(
                upload_url, chunk, chunk_headers
            )
            response.raise_for_status()
            start = end

    def _put_upload_chunk_with_retry(self, upload_url, chunk, headers):
        """PUTs a single upload-session chunk, retrying on transient HTTP errors
        (408/429/5xx) up to self.max_upload_retries, honoring Retry-After."""
        import time

        retryable_statuses = {408, 429, 500, 502, 503, 504}
        attempt = 0

        while True:
            response = self.session.put(
                upload_url,
                headers=headers,
                data=chunk,
                timeout=self.request_timeout_seconds,
            )

            if response.status_code not in retryable_statuses:
                return response

            attempt += 1
            if attempt > self.max_upload_retries:
                return response

            retry_after = self._retry_after_seconds(response, attempt)
            self.logger.warning(
                f"Retrying Graph attachment upload chunk after HTTP "
                f"{response.status_code}; attempt {attempt} of "
                f"{self.max_upload_retries}; sleeping {retry_after:.1f} seconds"
            )
            time.sleep(retry_after)

    @staticmethod
    def _retry_after_seconds(response, attempt: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), 0.0)
            except ValueError:
                pass
        return min(2 ** attempt, 30)
