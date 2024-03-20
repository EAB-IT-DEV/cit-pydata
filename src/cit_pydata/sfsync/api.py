import sys

from cit_pydata.util import api as util_api


class SFSyncClient:
    def __init__(self, sql_client, logger=None):
        self.logger = util_api.get_logger(__name__, "INFO") if not logger else logger

        self.sql_client = sql_client

        if self.sql_client.dialect != "pyodbc":
            self.logger.error(
                f"SQL Client dialect {self.sql_client.dialect} not supported for SFSync. Use pyodbc instead"
            )
            return

    def get_object(self, object_api_name, integration_name=None, retrieval_type=None):
        _stored_procedure = "usp_GetSalesforce_Object"
        _param_list = [object_api_name, integration_name, retrieval_type]
        self.logger.info(f"Executing SFSync get_object {_param_list}")
        result = self.sql_client.execute_stored_procedure(
            _stored_procedure, _param_list, skip_transaction=True
        )
        return result

    def get_object_group(
        self, object_group_name, retrieval_type, integration_name, **kwargs
    ):
        pass

    def load_object(self, load_instance_name, integration_name, operation):
        _stored_procedure = "usp_LoadSalesforce_Object"
        _param_list = [load_instance_name, integration_name, operation]
        self.logger.info(f"Executing SFSync load_object {_param_list}")
        result = self.sql_client.execute_stored_procedure(
            _stored_procedure, _param_list, skip_transaction=True
        )
        return result
