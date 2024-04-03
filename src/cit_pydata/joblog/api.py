import sys

from cit_pydata.util import api as util_api


class JobLogClient:
    def __init__(self, sql_client, job_name, logger=None):
        self.logger = util_api.get_logger(__name__, "INFO") if not logger else logger

        self.sql_client = sql_client

        self.job_name = job_name
        self.log_guid = None

    def start(self):
        import uuid

        if not self.log_guid:
            self.log_guid = str(uuid.uuid4())
            param_list = [self.job_name, "initializing", self.log_guid]
            self.sql_client.execute_stored_procedure("usp_Start_JobLog", param_list)
            self.logger.info(
                f"JOBLOG - Started Job {self.job_name} with GUID {self.log_guid}"
            )
            return self.log_guid
        else:
            self.logger.error(f"JOBLOG - Failed to start. Logging already started")

    def finish(self, is_success: bool = True, message: str = None):
        if is_success:
            result = "Success"
            self.logger.info(
                f'JOBLOG - SUCCESS - "{self.job_name}" with GUID {self.log_guid}'
            )
        else:
            result = "Failure"
            self.logger.info(
                f'JOBLOG - FAILED - "{self.job_name}" with GUID {self.log_guid}'
            )

        param_list = [self.log_guid, message, result]

        # Update JobLog with result
        self.sql_client.execute_stored_procedure("usp_Update_JobLog", param_list)

    def update_status(self, message):
        param_list = [self.log_guid, message, None]
        self.sql_client.execute_stored_procedure("usp_Update_JobLog", param_list)

    def update_joblog(sql_client, method, guid, status, logger=None):
        # If no sql_client supplied quietly ignore
        if sql_client:
            assert method in ["Success", "Failure", "Status"]

            if method in ["Success", "Failure"]:
                result = method
                param_list = [guid, status, result]
            elif method == "Status":
                param_list = [guid, status]

            stored_procedure_name = "usp_Update_JobLog"
            sql_client.execute_stored_procedure(stored_procedure_name, param_list)

    def insert_message(
        self,
        message_name,
        message_type,
        value_string: str = None,
        value_bit: bool = None,
        value_int: int = None,
        value_date=None,
    ):
        """
        INSERT INTO tbl_JobLogAttribute
                (JobLogGUID,AttributeName,AttributeType,AttributeLogType,ValueType,ValueString,ValueBit,ValueInteger,ValueDate)

                sample usage:
                job_log.insert_message('Job Parameter', 'Input Parameter', 'getActiveMembershipMainContact')
        """
        param_list = [
            self.log_guid,
            message_name,
            message_type,
            None,
            None,
            value_string,
            value_bit,
            value_int,
            value_date,
        ]
        self.logger.debug(f"JOBLOG - insert attribute param_list: {param_list}")
        # Update JobLog with result
        self.sql_client.execute_stored_procedure("usp_LogJobAttribute", param_list)
