import os
import sys

from cit_pydata.util import api as util_api
from cit_pydata.aws import api as aws_api


class SFTPClient:
    def __init__(self, conn: dict, logger=None):
        """
        conn = {
            'instance': 'sftp instance name', #e.g. 'getpaid'
            'port': 'number' #e.g. '22'
        }
        """
        # Handle logger
        self.logger = util_api.get_logger(__name__, "INFO") if not logger else logger
        self.base_ssm_parameter_name = conn.get("base_ssm_parameter_name")

        # Handle connection details
        self.instance = conn.get("instance", None)
        assert self.instance is not None

        self.port = conn.get("port", None)
        assert self.port is not None

        self.aws_environment = util_api.get_environment_variable(
            logger=self.logger, variable_name="aws_auth_environment"
        )

        self.aws_iam_user = util_api.get_environment_variable(
            logger=self.logger, variable_name="aws_auth_iam_user"
        )

        # Instantiate Session when using
        self.sftp_session = None

    def _connect(self):
        """Method for connecting to SFTP Server
        Called from all loading/downloading methods"""
        import paramiko

        try:
            aws_ssm_client = aws_api.SSMClient(
                environment=self.aws_environment,
                iam_user=self.aws_iam_user,
                logger=self.logger,
            )
        except Exception as e:
            self.logger.exception(e)
            return False

        sftp_hostname_parameter_name = (
            self.base_ssm_parameter_name + self.instance + "/host"
        )
        sftp_username_parameter_name = (
            self.base_ssm_parameter_name + self.instance + "/username"
        )
        sftp_password_parameter_name = (
            self.base_ssm_parameter_name + self.instance + "/password"
        )

        """"""
        sftp_hostname = None
        try:
            sftp_hostname = aws_ssm_client.get_parameter(
                name=sftp_hostname_parameter_name
            )
        except:
            self.logger.error(
                f"Unable to get SFTP Hostname from AWS SSM {sftp_hostname_parameter_name}"
            )
            return False

        sftp_username = None
        try:
            sftp_username = aws_ssm_client.get_parameter(
                name=sftp_username_parameter_name
            )
        except:
            self.logger.error(
                f"Unable to get SFTP Username from AWS SSM {sftp_username_parameter_name}"
            )
            return False

        sftp_password = None
        try:
            sftp_password = aws_ssm_client.get_parameter(
                name=sftp_password_parameter_name, with_decryption=True
            )
        except:
            self.logger.error(
                f"Unable to get SFTP Password from AWS SSM {sftp_password_parameter_name}"
            )
            return False

        assert sftp_password is not None
        assert sftp_username is not None
        assert sftp_hostname is not None

        # Initialize instance of the SHHClient
        self.sftp_client = paramiko.SSHClient()

        # Connect to SFTP Server
        try:
            # Automatically add the server's host key
            self.sftp_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # Connect to the SFTP server and open an SFTP session on the SSHClient
            self.sftp_client.connect(
                sftp_hostname, self.port, sftp_username, sftp_password
            )
            self.sftp_session = self.sftp_client.open_sftp()

            log_message = f"Successfully connected to {self.instance} SFTP server."
            self.logger.info(log_message)
            # self.logger.debug('Hooray!')
        except paramiko.AuthenticationException as auth_exception:
            self.logger.error(
                f"Authentication failed for the {self.instance} SFTP Server: {auth_exception}"
            )
            return False
            # return

        except paramiko.SSHException as ssh_exception:
            self.logger.error(
                f"SSH error occurred while connecting to the {self.instance} SFTP server: {ssh_exception}"
            )
            return False
            # return

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
            return False
            # return

        return True

    def load_file_to_sftp(
        self,
        target_folder_path,
        source_folder_path,
        source_file_name,
        target_file_name=None,
        archive_folder_path=None,
        archive_suffix=None,
        auto_close=True,
    ):
        """Load a single file from the source to the target folder on the SFTP server.
        If archive_folder_path is None, overwrite the existing file on the server.
        If archive_folder_path is provided, move the existing file to the archive folder
        with an optional archive_suffix in the filename.
        Returns True if the file transfer is successful, False otherwise.
        Default behavior is to automatically close the connection after load"""
        from datetime import datetime

        if self.sftp_session is None:
            connected = self._connect()
            if not connected:
                # logging for Failure already exists in _connect()
                return False

        try:
            # Check if file exists locally
            filepath = os.path.join(source_folder_path, source_file_name)
            if not os.path.isfile(filepath):
                self.logger.error(f"Source file {filepath} does not exist locally.")
                return False

            # Check if folder path exists in SFTP server
            try:
                self.sftp_session.chdir(target_folder_path)
            except IOError as error:
                self.logger.error(f"Folder path {target_folder_path} does not exist.")
                return False

            # Use source_file_name as target_file_name if not provided
            target_file_name = target_file_name or source_file_name

            # Archive if target file already exists and there is an archive destination
            try:
                self.sftp_session.stat(target_file_name)

                if archive_folder_path is None:
                    # Overwrite the existing file
                    self.sftp_session.put(
                        filepath,
                        os.path.join(target_folder_path, target_file_name).replace(
                            os.sep, "/"
                        ),
                    )
                    self.logger.info(
                        f'File {source_file_name} overwritten in {os.path.join(target_folder_path, target_file_name).replace(os.sep, "/")}'
                    )
                else:
                    # Archive the existing file
                    archive_suffix = archive_suffix or datetime.now().strftime("%Y%m%d")
                    archived_filename = f"{os.path.splitext(target_file_name)[0]}_{archive_suffix}{os.path.splitext(target_file_name)[1]}"
                    self.sftp_session.rename(
                        target_file_name,
                        os.path.join(archive_folder_path, archived_filename).replace(
                            os.sep, "/"
                        ),
                    )
                    self.logger.info(
                        f"Target file {target_file_name} archived as {archived_filename}"
                    )
            except FileNotFoundError:
                # File doesn't exist, just copy to the target folder
                pass
            except Exception as e:
                self.logger.error(f"Error with archiving file: {e}")
                return False

            # Copy file over
            try:
                self.sftp_session.put(
                    filepath,
                    os.path.join(target_folder_path, target_file_name).replace(
                        os.sep, "/"
                    ),
                )
                self.logger.info(
                    f'File {source_file_name} loaded to {os.path.join(target_folder_path, target_file_name).replace(os.sep, "/")}'
                )
                return True
            except Exception as e:
                self.logger.error(f"Error with copying file: {e}")
                return False
        finally:
            if auto_close:
                self._close_connection()

    def download_file_from_sftp(
        self,
        source_folder_path,
        source_file_name,
        target_folder_path,
        target_file_name=None,
        archive_folder_path=None,
        archive_suffix=None,
        auto_close=True,
    ):
        """Download a single file from the source folder on the SFTP server to the local target folder.
        If archive_folder_path is provided, move the existing file on the server
        to the archive folder with an optional archive_suffix in the filename.
        Returns True if the file download is successful, False otherwise.
        Default behavior is to automatically close the connection after download."""
        import shutil
        from datetime import datetime

        if self.sftp_session is None:
            connected = self._connect()
            if not connected:
                # logging for Failure already exists in _connect()
                return False

        try:
            # Check if source folder path exists on SFTP server
            try:
                self.sftp_session.chdir(source_folder_path)
            except FileNotFoundError:
                self.logger.error(
                    f"Source folder path {source_folder_path} does not exist on the SFTP server."
                )
                return False
            except Exception as error:
                self.logger.error(
                    f"Error changing directory on the SFTP server: {error}"
                )
                return False

            # Check if source file exists on SFTP server
            try:
                self.sftp_session.stat(source_file_name)
            except FileNotFoundError:
                self.logger.error(
                    f"Source file {source_file_name} does not exist on the SFTP server."
                )
                return False
            except Exception as error:
                self.logger.error(
                    f"Error checking file existence on the SFTP server: {error}"
                )
                return False

            # Check if target folder path exists
            if not os.path.exists(target_folder_path):
                self.logger.error(
                    f"The local folder path {target_folder_path} does not exist."
                )
                return False

            # Use source_file_name as target_file_name if not provided
            target_file_name = target_file_name or source_file_name

            # Check if file exists locally
            target_filepath = os.path.join(target_folder_path, target_file_name)
            try:
                if os.path.isfile(target_filepath):
                    try:
                        if archive_folder_path is None:
                            # Overwrite the existing local file
                            self.sftp_session.get(
                                source_file_name, target_filepath.replace(os.sep, "/")
                            )
                            self.logger.info(
                                f'File {target_file_name} overwritten in {target_filepath.replace(os.sep, "/")}'
                            )
                            return True
                        else:
                            # Archive the existing file
                            archive_suffix = archive_suffix or datetime.now().strftime(
                                "%Y%m%d"
                            )
                            archived_filename = f"{os.path.splitext(target_file_name)[0]}_{archive_suffix}{os.path.splitext(target_file_name)[1]}"
                            archive_filepath = os.path.join(
                                archive_folder_path, archived_filename
                            ).replace(os.sep, "/")
                            if os.path.exists(archive_folder_path):
                                # archive path exists, pass to archiving
                                pass
                            else:
                                self.logger.error(
                                    f"Archive folder path {archive_folder_path} does not exist."
                                )
                                return False
                            shutil.move(target_filepath, archive_filepath)
                            self.logger.info(
                                f"File {target_file_name} archived in {archive_filepath}"
                            )
                    except Exception as error:
                        self.logger.error(f"Error with archiving file: {error}")
                        return False
            except Exception as error:
                self.logger.error(f"Error checking file existence locally: {error}")

            # Download the file
            try:
                self.sftp_session.get(
                    source_file_name,
                    os.path.join(target_folder_path, target_file_name).replace(
                        os.sep, "/"
                    ),
                )
                self.logger.info(
                    f'File {source_file_name} downloaded to {os.path.join(target_folder_path, target_file_name).replace(os.sep, "/")}'
                )
                return True
            except Exception as error:
                self.logger.error(f"Error with downloading file: {error}")
                return False
        finally:
            if auto_close:
                self._close_connection()

    def _close_connection(self):
        # Close the SFTP session
        if self.sftp_session:
            try:
                self.sftp_session.close()
                self.logger.info(f"SFTP session closed")
                self.sftp_session = None
                pass
            except Exception as e:
                self.logger.error(f"Error with closing session: {e}")
        # Close the SSH client
        if self.sftp_client.get_transport():
            try:
                self.sftp_client.close()
                self.logger.info(f"SFTP client connection closed")
            except Exception as e:
                self.logger.error(f"Error with closing client: {e}")
        else:
            self.logger.info(
                f"Attempted to close SFTP Client connection that was already closed"
            )
