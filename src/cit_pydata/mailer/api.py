import base64
import os
import re

from cit_pydata.util import api as util_api
from cit_pydata.aws import api as aws_api

EMAIL_PATTERN = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
FILE_ALIAS_PATTERN = re.compile(r"^[a-zA-Z0-9_-][a-zA-Z0-9_. -]*[a-zA-Z0-9_-]$")

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
# Attachments above this size require a Graph upload session rather than an
# inline base64 fileAttachment. sendMail supports inline attachments up to ~3 MB.
MAX_INLINE_ATTACHMENT_BYTES = 3 * 1024 * 1024


class MailClient:
    """
    Sends email via the Microsoft Graph API (sendMail endpoint).

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

    def _build_attachment(self, attachment, attachment_alias: str = None) -> dict:
        """
        Builds a Graph fileAttachment dict from a filepath (str) or an object
        exposing to_csv() (e.g. a pandas DataFrame).
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

        if len(content_bytes) > MAX_INLINE_ATTACHMENT_BYTES:
            raise ValueError(
                f'Attachment "{filename}" exceeds the {MAX_INLINE_ATTACHMENT_BYTES} '
                "byte limit for inline Graph attachments"
            )

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

    def send_email(
        self,
        sender_email: str,
        receiver_email: str,
        subject: str,
        body: str = None,
        cc_email: str = None,
        attachment=None,
        attachment_alias: str = None,
        body_type: str = "HTML",
        save_to_sent_items: bool = False,
    ) -> bool:
        """
        Sends an email via Microsoft Graph. Returns True on success, False on failure.

        sender_email must be a mailbox the app registration is authorized to send
        as. receiver_email/cc_email accept comma-separated addresses.
        attachment can be a filepath (str) or an object with a to_csv() method
        (e.g. a pandas DataFrame). attachment_alias renames the attachment to
        something more recipient-friendly (e.g. "Contract_Details").
        body_type is "HTML" or "Text".
        """
        import requests

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

        attachment_label = None
        if attachment is not None:
            try:
                file_attachment = self._build_attachment(attachment, attachment_alias)
            except ValueError as e:
                self.logger.error(e)
                return False
            message["attachments"] = [file_attachment]
            attachment_label = file_attachment["name"]

        try:
            self._authenticate()
        except Exception:
            return False

        self.logger.info(
            f"Preparing to send email via Graph: subject={subject}, sender={sender_email}, "
            f"receiver={receiver_email}, cc={cc_email}, attachment={attachment_label}"
        )

        send_mail_url = f"{GRAPH_BASE_URL}/users/{sender_email}/sendMail"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }
        payload = {"message": message, "saveToSentItems": save_to_sent_items}

        try:
            response = requests.post(send_mail_url, headers=headers, json=payload)
            response.raise_for_status()
            self.logger.info("Email sent successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error occurred while sending the email: {e}")
            if "response" in dir() and getattr(response, "text", None):
                self.logger.error(response.text)
            return False
