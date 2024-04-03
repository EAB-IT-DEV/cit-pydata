import os
import sys

from cit_pydata.util import api as util_api
from cit_pydata.aws import api as aws_api


class MarketoClient:
    def __init__(self, conn: dict, logger=None):
        """
        Custom Marketo Client for all Marketo API needs.
        Exposes the package MarketoRestPython via the member "marketo"
        conn = {
            'instance':'production'|'sandbox',
            'app': 'launchpoint app name',
        }
        """

        self.logger = util_api.get_logger(__name__, "INFO") if not logger else logger
        self.base_ssm_parameter_name = conn.get("base_ssm_parameter_name")

        # Handle connection details
        self.instance = conn.get("instance", None)
        assert self.instance in ["production", "sandbox"]

        self.app = conn.get("app", None)
        assert self.app is not None

        self.aws_environment = util_api.get_environment_variable(
            logger=self.logger, variable_name="aws_auth_environment"
        )

        self.aws_iam_user = util_api.get_environment_variable(
            logger=self.logger, variable_name="aws_auth_iam_user"
        )

        # assert self.aws_environment is not None
        # assert self.aws_iam_user is not None

        self.marketo = None

        self.CUSTOM_OBJECT_STANDARD_FIELDS = [
            "marketoGUID",
            "updatedAt",
            "createdAt",
            "uniqueId",
            "linkId",
        ]

    def _authenticate(self):
        from marketorestpython.client import MarketoClient as marketorestpython

        if self.marketo is None:
            try:
                aws_ssm_client = aws_api.SSMClient(
                    environment=self.aws_environment, iam_user=self.aws_iam_user
                )
            except Exception as e:
                self.logger.exception(e)

            marketo_munchkin_id_parameter_name = (
                self.base_ssm_parameter_name + self.instance + "/munchkin_id"
            )
            marketo_client_id_parameter_name = (
                self.base_ssm_parameter_name
                + self.instance
                + "/app/"
                + self.app
                + "/client_id"
            )
            marketo_client_secret_parameter_name = (
                self.base_ssm_parameter_name
                + self.instance
                + "/app/"
                + self.app
                + "/client_secret"
            )

            munchkin_id = None
            try:
                munchkin_id = aws_ssm_client.get_parameter(
                    marketo_munchkin_id_parameter_name
                )
            except:
                self.logger.error(
                    f"Unable to get Munchkin Id from AWS SSM {marketo_munchkin_id_parameter_name}"
                )
                return

            client_id = None
            try:
                client_id = aws_ssm_client.get_parameter(
                    marketo_client_id_parameter_name
                )
            except:
                self.logger.error(
                    f"Unable to get Marketo Client Id from AWS SSM {marketo_client_id_parameter_name}"
                )
                return

            client_secret = None
            try:
                client_secret = aws_ssm_client.get_parameter(
                    marketo_client_secret_parameter_name, with_decryption=True
                )
            except:
                self.logger.error(
                    f"Unable to get Marketo Client Secret from AWS SSM {marketo_client_secret_parameter_name}"
                )
                return

            api_limit = None
            max_retry_time = None
            self.marketo = marketorestpython(
                munchkin_id=munchkin_id,
                client_id=client_id,
                client_secret=client_secret,
                api_limit=api_limit,
                max_retry_time=max_retry_time,
            )
            self.logger.info(f"Connected to Marketo instance {munchkin_id}")

    def execute(self, **kwargs):
        self._authenticate()
        # self.logger.debug(f'execute({kwargs})...')
        response = None
        try:
            response = self.marketo.execute(**kwargs)
            # self.logger.debug(f'\texecute() -> {response}...')
        except Exception as e:
            self.logger.error(f"API call failed: {str(e)}")
            # self.logger.exception(f'{str(e)}')

        return response

    def execute_api_call(self, method, endpoint, *args, **kwargs):
        import json

        self._authenticate()
        self.marketo.authenticate()

        api_args = {}
        if len(args) > 0:
            api_args.update(args)

        data = kwargs.get("body", None)

        self.logger.info(f"Sending API Call {str(method).upper()}: {endpoint} ")
        self.logger.info(f"\tParams: {json.dumps(api_args)} ")
        # self.logger.debug(f'\tBody: {json.dumps(data)} ')

        api_args["access_token"] = self.marketo.token
        result = None

        try:
            result = self.marketo._api_call(
                method, self.marketo.host + endpoint, api_args, data
            )
            if result is None:
                self.logger.info("Empty response")
                return False, None
        except Exception as e:
            self.logger.info(f"API Call Failed: {str(e)}")
            return False, None

        self.logger.debug("\tAPI call success")
        return True, result

    def submit_form(self, form_payload: dict):
        import requests

        self._authenticate()
        self.marketo.authenticate()
        self.logger.debug(self.marketo.token)
        auth_header = f"Bearer {self.marketo.token}"
        headers = {"Content-Type": "application/json", "Authorization": auth_header}
        url = self.marketo.host + "/rest/v1/leads/submitForm.json"
        self.logger.debug(url)
        self.logger.debug(form_payload)
        request = requests.post(url=url, headers=headers, json=form_payload)
        self.logger.debug(request.status_code)
        self.logger.debug(request.text)

    def get_program(self, program_id):
        import json

        self._authenticate()

        try:
            lead = self.client.execute(method="get_program_by_id", id=program_id)
        except KeyError:
            lead = False

        if lead:
            self.logger.debug(json.dumps(lead, indent=4, sort_keys=True))

    def get_lead_fields(self):
        self._authenticate()
        args = {"access_token": self.marketo.token}

        result = self.marketo._api_call(
            "get", self.marketo.host + "/rest/v1/leads/schema/fields.json", args
        )
        if result is None:
            raise Exception("Empty Response")
        return result["result"]

    def get_lead_fields2(self):
        import json

        self._authenticate()
        lead = self.marketo.execute(method="describe2")

        file_path = "src/core/marketo/data/out/marketo_lead_describe2.json"
        util_api.remove_file(file_path)
        with open(file_path, "w") as json_file:
            for item in lead:
                json.dump(item, json_file)

    def get_custom_object_records(
        self, custom_object_name, filter_type, filter_list, custom_field_list=[]
    ):
        """
        Returns DataFrame of Custom Object records based on filter criteria
        """
        import pandas as pd

        batch_size = 300
        field_list = self.CUSTOM_OBJECT_STANDARD_FIELDS
        for field in custom_field_list:
            field_list.append(field)

        df = pd.DataFrame()

        # marketo can only retrieve 300 records at a time
        for i in range(0, len(filter_list), batch_size):
            batch_filter_list = filter_list[i : i + batch_size]
            batch_result = []
            try:
                batch_result = self.execute(
                    method="get_custom_objects",
                    input=batch_filter_list,
                    name=custom_object_name,
                    filterType=filter_type,
                    fields=field_list,
                    batchSize=None,
                )
            except Exception as e:
                self.logger.error(
                    f"Failed to get Marketo Custom Object records: {str(e)}"
                )
                return

            if len(batch_result) > 0:
                batch_df = pd.DataFrame(batch_result)
                batch_df = batch_df.drop(columns="seq")
                df = df.append(batch_df)

        # pd.set_option('display.max_columns', None)
        # column_list = result[0].keys()
        # self.logger.debug(column_list)

        return df

        # date_time_string = util_api.get_datetime_string()
        # save_file_name = f'{custom_object_name}_query_result_{date_time_string}.csv'
        # folder_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','archive',save_file_name)
        # util_api.list_dict_to_csv(folder_path, column_list, result)

    def sync_custom_object_records(self, sql_client, custom_object_name):
        """
        Upserts Custom Object data to Marketo

        This method is not complete:
        - upsert_co_query_dict needs to be updated with full queries for all custom object e.g. "Select * from vw_CustomObject_ProjectRole"
        """

        upsert_co_query_dict = {
            "staffAssignmentV2_c": "Select top 1 linkId, uniqueId, StaffAssignmentName as staffAssignmentName from [vw_CustomObject_GetStaffAssignment_v2] where linkid = 'N00137649'",
            "projectRole_c": "Select top 1 linkId, uniqueId from [vw_CustomObject_ProjectRole] where linkid = 'N00137649'",
        }

        query = upsert_co_query_dict.get(custom_object_name, None)
        if query is None or query == "":
            self.logger.errror(f"Custom Object not supported {custom_object_name}")
            return

        self.logger.info(f"Executing SQL Query: {query}")

        # get custom object data from sql in dataframe
        try:
            df = sql_client.get_dataframe_query(query)
        except Exception as e:
            self.logger.error(f"Failed to get SQL data: {str(e)}")
            return

        co_record_dict_list = df.to_dict("records")
        # self.logger.debug(records)

        # save source data as file
        date_time_string = util_api.get_datetime_string()
        save_file_name = f"{custom_object_name}_data_{date_time_string}.csv"
        folder_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "data", "archive"
        )
        util_api.list_dict_to_csv(
            folder_path,
            save_file_name,
            column_list=list(df.columns),
            data_list=co_record_dict_list,
        )

        try:
            result = self.execute(
                method="create_update_custom_objects",
                name=custom_object_name,
                input=co_record_dict_list,
                action="createOrUpdate",
                dedupeBy=None,
            )
            self.logger.debug(result)
        except Exception as e:
            self.logger.error(f'Failed to create CO recods" {str(e)}')
            return

        # save results
        save_file_name = f"{custom_object_name}_result_{date_time_string}.csv"
        folder_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "data", "archive"
        )
        util_api.list_dict_to_csv(
            folder_path,
            save_file_name,
            data_list=result,
            column_list=["seq", "marketoGUID", "status"],
        )

        # self.logger.debug(result)

    def _delete_co_df(
        self, custom_object_name: str, filter_list: list, delete_by, log_path=None
    ):
        """
        Deletes Marketo custom object records with filter_list and delete_by arguments.
        """
        import pandas as pd

        self.logger.info(
            f"Deleting {len(filter_list)} {custom_object_name} custom object records"
        )
        df = pd.DataFrame()
        batch_size = 300

        # marketo can only delete 300 records at a time
        for i in range(0, len(filter_list), batch_size):
            batch_filter_list = filter_list[i : i + batch_size]
            batch_result = []

            try:
                batch_result = self.execute(
                    method="delete_custom_objects",
                    name=custom_object_name,
                    input=batch_filter_list,
                    deleteBy=delete_by,
                )
            except Exception as e:
                self.logger.error(
                    f"Failed to delete Marketo custom object record batch {str(e)}"
                )
                batch_result = []

            if len(batch_result) > 0:
                batch_df = pd.DataFrame(batch_result)
                batch_df = batch_df.drop(columns="seq")
                df = df.append(batch_df)

        # log result of deletion
        if log_path is not None:
            date_time_string = util_api.get_datetime_string()

            # save records to delete
            save_file_name = f"{custom_object_name}_to_delete_{date_time_string}.csv"
            create_file_success = util_api.list_dict_to_csv(
                log_path, save_file_name, data_list=filter_list
            )

            if create_file_success:
                self.logger.info(f"Created csv file {save_file_name}")
            else:
                self.logger.error(f"Failed to create csv file {save_file_name}")

            # save deleted records
            save_file_name = (
                f"{custom_object_name}_delete_result_{date_time_string}.csv"
            )
            file_path = os.path.join(log_path, save_file_name)
            df.to_csv(file_path)

        return True

    def delete_custom_object_records(
        self, sql_client, custom_object_name: str, log_path: str = None
    ):
        """Deletes Custom Object records that should not exist in Marketo.

        In order to delete CO records in Marketo first we get all Marketo leads/contacts that shouldn't have any CO records and we delete those.
        Next for leads/contacts that have at least one CO records, we compare existing CO records to all CO records and filter out good records
        """
        import pandas as pd

        # this dict contains the sql queries for fetching the lead counter ids in order to interrogate
        #   the Marketo custom object records and search for records to delete
        # leads_wo_co_query = must return a single column with name "linkId" containing the linking value for the Mkto Custom Object
        # all_co_records_query = must return the linkId and uniqueId for all source custom objects.
        #     this data set will be the source of truth for what CO records should exist in Marketo.
        delete_co_query_dict = {
            "projectRole_c": {
                "leads_wo_co_query": "select counter_id__c as linkId from SFReplication_Full2..Contact c left join [vw_CustomObject_GetProjectRole] co on c.Counter_ID__c = co.linkId where entity__c != 'hc' and Contact_Status__c = 'active' and email is not null and co.linkId is null",
                "all_co_records_query": "select linkId, uniqueId FROM vw_CustomObject_GetProjectRole",
            },
            "staffAssignmentV2_c": "",
        }

        # get and validate sql queries from configuration
        co_query_dict = delete_co_query_dict.get(custom_object_name, None)

        if co_query_dict is None:
            self.logger.error(f'Custom Object not supported: "{custom_object_name}"')
            return

        leads_wo_co_query = co_query_dict.get("leads_wo_co_query", None)
        if leads_wo_co_query is None:
            self.logger.error(
                f'Query not configured for key {custom_object_name}["leads_wo_co_query"]'
            )
            return

        all_co_records_query = co_query_dict.get("all_co_records_query", None)
        if all_co_records_query is None:
            self.logger.error(
                f'Query not configured for key {custom_object_name}["all_co_records_query"]'
            )
            return

        self.logger.info(
            f"Checking Custom Object {custom_object_name} for records to delete"
        )

        # init DataFrame
        co_records_delete_df = pd.DataFrame()

        # get source data from sql into dataframe
        try:
            df = sql_client.get_dataframe_query(leads_wo_co_query)
        except Exception as e:
            self.logger.error(f"Failed to get SQL data: {str(e)}")
            return

        # if leads/contacts exist that do not have any CO records
        if df is not None and df.empty is False:
            leads_wo_co_dict_list = df.to_dict("records")
            _filter_type = "linkId"

            self.logger.info(
                f"Getting custom object records for {len(leads_wo_co_dict_list)} Marketo leads"
            )
            co_records_delete_df = self.get_custom_object_records(
                custom_object_name,
                filter_type=_filter_type,
                filter_list=leads_wo_co_dict_list,
            )

            # if there are CO records to delete
            if co_records_delete_df is not None and co_records_delete_df.empty is False:
                # get only uniqueId field from custom object
                co_records_delete_df = co_records_delete_df[["uniqueId"]]
                self.logger.info(
                    f"\t...retrieved {co_records_delete_df.size} records to delete"
                )

        # Next step, get all CO records for leads that have a CO record.
        #   For each lead/contact get all CO records in Marketo and verify records are accurate

        # get all custom object records from sql
        try:
            all_source_co_records_df = sql_client.get_dataframe_query(
                all_co_records_query
            )
        except Exception as e:
            self.logger.error(f"Failed to get SQL data: {str(e)}")
            return

        if (
            all_source_co_records_df is not None
            and all_source_co_records_df.empty is False
        ):
            # for each source contact/lead get all marketo CO records.
            all_source_co_records_df.uniqueId = (
                all_source_co_records_df.uniqueId.astype(str)
            )
            all_source_co_records_df.linkId = all_source_co_records_df.linkId.astype(
                str
            )

            co_leads_df = all_source_co_records_df["linkId"].drop_duplicates()
            _filter_type = "linkId"
            co_leads_to_validate_dict_list = [
                {"linkId": value} for value in co_leads_df.to_dict().values()
            ]

            self.logger.info(
                f"Getting scustom object records for {len(co_leads_to_validate_dict_list)} Marketo leads"
            )
            co_records_to_validate_df = self.get_custom_object_records(
                custom_object_name,
                filter_type=_filter_type,
                filter_list=co_leads_to_validate_dict_list,
            )
            self.logger.info(
                f"Retrieved {co_records_to_validate_df.size} Custom Object records for validation"
            )

            co_records_to_validate_df = co_records_to_validate_df[
                ["linkId", "uniqueId"]
            ]
            co_records_to_validate_df.set_index("uniqueId")
            co_records_to_validate_df.linkId = co_records_to_validate_df.linkId.astype(
                str
            )
            co_records_to_validate_df.linkId = co_records_to_validate_df.linkId.astype(
                str
            )

            # join marketo co records with source sf records to get co records to delete
            # linkId_r are the lookups from SQL, linkId_l are the lookups from Marketo
            # we want to delete records in Marketo that don't have a lookup in SQL
            compare_df = co_records_to_validate_df.merge(
                all_source_co_records_df.reset_index(),
                on=["uniqueId"],
                how="left",
                suffixes=("_l", "_r"),
            )

            # select the 3 fields we care about
            compare_df = compare_df[["uniqueId", "linkId_l", "linkId_r"]]

            # select only records wo a lookup in SQL
            to_delete_df = compare_df[compare_df["linkId_r"].isna()]
            to_delete_df = to_delete_df[["uniqueId"]]
            self.logger.info(f"Retrieved {to_delete_df.size} CO records to delete")

            # collect all records to delete
            if co_records_delete_df.empty is True:
                co_records_delete_df = to_delete_df
            elif co_records_delete_df.empty is False:
                co_records_delete_df = co_records_delete_df.append(to_delete_df)
            else:
                self.logger.info(f"No Custom Object records found to be deleted")
                return None

            self.logger.info(f"CO records to delete head: {to_delete_df.head()}")
            self.logger.info(f"CO records to delete size: {to_delete_df.size}")

            delete_by = "dedupeFields"
            records_to_delete_list = df.to_dict("records")
            self._delete_co_df(
                custom_object_name,
                filter_list=records_to_delete_list,
                delete_by=delete_by,
                log_path=log_path,
            )
        else:
            self.logger.error("No source Custom Object records exist. Investigate.")


# def get_activities():
#     try:
#         lead = mc.execute(method='get_lead_activities', activityTypeIds=['2'], nextPageToken=None,
#             sinceDatetime='2020-09-01', untilDatetime='2020-09-02',
#             batchSize=None, listId=None, leadIds=[7438441])
#     except KeyError:
#         lead = False
#         self.logger.debug(json.dumps(lead, indent=4, sort_keys=True))
