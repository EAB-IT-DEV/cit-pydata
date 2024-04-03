import os
import sys

from cit_pydata.util import api as util_api


class BoxClient:
    def __init__(self, box_jwt_auth_file_path, logger=None):
        self.logger = util_api.get_logger(__name__, "INFO") if not logger else logger

        self.box_jwt_auth_file_path = box_jwt_auth_file_path

        # client is the boxsdk Client object
        self.client = None

    def _get_client(self):
        from boxsdk import Client, JWTAuth, OAuth2

        # JWT Auth
        auth = JWTAuth.from_settings_file(self.box_jwt_auth_file_path)

        try:
            self.client = Client(auth)
        except Client.BoxAPIException as err:
            self.logger.error("Failed to Authenticate to Box API:", err)

    def get_folder(self, folder_id: str):
        if not self.client:
            self._get_client()

        return self.client.folder(folder_id=folder_id)

    def get_file(self):
        pass

    def get_folder_items(self, folder_id: str, filter_type: str = None):
        if not self.client:
            self._get_client()

        item_id_dict = {}
        items = self.client.folder(folder_id=folder_id).get_items()

        assert filter_type in ["file", "folder", None]
        if filter_type == "file":
            for item in items:
                if item.type == filter_type:
                    item_id_dict[item.id] = item
        elif filter_type == "folder":
            for item in items:
                if item.type == filter_type:
                    item_id_dict[item.id] = item
        else:
            for item in items:
                item_id_dict[item.id] = item

        for item in items:
            if item.type == filter_type:
                item_id_dict[item.id] = {"name": item.name, "type": item.type}
            else:
                item_id_dict[item.id] = {"name": item.name, "type": item.type}

        return item_id_dict

    def get_shared_link(self, folder_id):
        if not self.client:
            self._get_client()

        folder = self.get_folder(folder_id)
        shared_link = folder.get().shared_link
        return shared_link

    def copy_file(self, source_file_id, target_folder_id):
        """
        Copies Box fild to another folder
            source_file_id - Box file id of the file to copy
            target_folder_id - Box folder id of where to copy the file too
        """
        if not self.client:
            self._get_client()

        file_to_copy = self.client.file(source_file_id)
        target_folder = self.client.folder(target_folder_id)
        try:
            file_copy = file_to_copy.copy(target_folder)
            self.logger.info(
                'File "{0}" has been copied into folder "{1}"'.format(
                    file_copy.name, file_copy.parent.name
                )
            )
        except Exception as e:
            self.logger.error(f"Failed to copy Box item {e}")

    def upload_file(self, file_path, box_folder_id):
        if not self.client:
            self._get_client()

        if os.path.exists(file_path):
            new_file = self.client.folder(box_folder_id).upload(file_path)
            self.logger.info(
                'File "{0}" uploaded to Box with file ID {1}'.format(
                    new_file.name, new_file.id
                )
            )
            return True
        else:
            self.logger.error(f"File does not exist {file_path}")
            return False
