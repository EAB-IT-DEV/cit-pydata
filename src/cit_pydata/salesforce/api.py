import os
import sys

from cit_pydata.util import api as util_api
from cit_pydata.aws import api as aws_api


def get_field_value_from_relationship_lookup(lookup_dict, field_api_name):
    assert isinstance(lookup_dict, dict)
    assert field_api_name
    return lookup_dict[field_api_name]


class SalesforceClient:
    def __init__(self, conn: dict, logger=None):
        """
        conn = {
            'instance':'sf instance name',
            'user':'sf user',
            'app': 'sf connected app',
        }
        """

        # Handle logger
        self.logger = util_api.get_logger(__name__, "INFO") if not logger else logger

        self.base_ssm_parameter_name = conn.get("base_ssm_parameter_name")

        # Handle connection details
        self.instance = conn.get("instance", None)
        assert self.instance is not None

        self.is_sandbox = True if self.instance != "prod" else False
        self.user = conn.get("user", None)
        self.app = conn.get("app", None)

        assert self.user is not None
        assert self.app is not None

        self.aws_environment = util_api.get_environment_variable(
            logger=self.logger, variable_name="aws_auth_environment"
        )

        self.aws_iam_user = util_api.get_environment_variable(
            logger=self.logger, variable_name="aws_auth_iam_user"
        )

        # assert self.aws_environment is not None
        # assert self.aws_iam_user is not None

        # Do not get access token during class instantiation. Wait for utilization before getting access token.
        self.sf_token = None
        self.access_token = None
        self.instance_url = None
        self.ssf = None

    def _authenticate(self):
        """
        Returns dictionary of OAuth acces_token and instance_url for Salesforce Rest API
        {
            'access_token': 'zzzzz',
            'instance_url': 'https://sf--catalyst.my.salesforce.com',
            'id': 'https://test.salesforce.com/id/00D040100004eqgBAL/0052K00000Aox3CZAR',
            'token_type': 'Bearer', 'issued_at': '1629746368808',
            'signature': 'zzz'
        }
        """
        import requests

        if self.sf_token is None:
            try:
                aws_ssm_client = aws_api.SSMClient(
                    environment=self.aws_environment,
                    iam_user=self.aws_iam_user,
                    logger=self.logger,
                )
            except Exception as e:
                self.logger.exception(e)

            sf_username_parameter_name = (
                self.base_ssm_parameter_name
                + self.instance
                + "/"
                + self.user
                + "/username"
            )
            sf_password_parameter_name = (
                self.base_ssm_parameter_name
                + self.instance
                + "/"
                + self.user
                + "/password"
            )
            sf_security_token_parameter_name = (
                self.base_ssm_parameter_name
                + self.instance
                + "/"
                + self.user
                + "/security_token"
            )
            sf_client_id_parameter_name = (
                self.base_ssm_parameter_name
                + self.instance
                + "/app/"
                + self.app
                + "/client_id"
            )
            sf_client_secret_parameter_name = (
                self.base_ssm_parameter_name
                + self.instance
                + "/app/"
                + self.app
                + "/client_secret"
            )

            sf_username = None
            try:
                sf_username = aws_ssm_client.get_parameter(sf_username_parameter_name)
            except:
                self.logger.error(
                    f"Unable to get SF username from AWS SSM {sf_username_parameter_name}"
                )

            sf_password = None
            try:
                sf_password = aws_ssm_client.get_parameter(
                    sf_password_parameter_name, with_decryption=True
                )
            except:
                self.logger.error(
                    f"Unable to get Salesforce password from AWS SSM {sf_password_parameter_name}"
                )

            sf_client_id = None
            try:
                sf_client_id = aws_ssm_client.get_parameter(sf_client_id_parameter_name)
            except:
                self.logger.error(
                    f"Unable to get SF username from AWS SSM {sf_client_id_parameter_name}"
                )

            sf_client_secret = None
            try:
                sf_client_secret = aws_ssm_client.get_parameter(
                    sf_client_secret_parameter_name, with_decryption=True
                )
            except:
                self.logger.error(
                    f"Unable to get SF username from AWS SSM {sf_client_secret_parameter_name}"
                )

            sf_security_token = ""
            try:
                sf_security_token = aws_ssm_client.get_parameter(
                    sf_security_token_parameter_name, with_decryption=True
                )
            except:
                self.logger.warning(
                    f"Unable to get SF User security token from AWS SSM {sf_security_token_parameter_name}. \
                                    Will attempt to login without it but will fail if the ip of the server running is not whitelisted on SF"
                )
            if not sf_security_token:
                sf_security_token = ""

            # Create payload for retrieving OAuth access token
            payload = {
                "grant_type": "password",
                "client_id": sf_client_id,
                "client_secret": sf_client_secret,
                "username": sf_username,
                "password": sf_password + sf_security_token,
            }
            # Get OAuth access token
            with requests.Session() as session:
                # headers={'Content-Type': 'application/json'}
                headers = {"Content-type": "application/x-www-form-urlencoded"}
                if self.instance == "prod":
                    r = session.post(
                        "https://login.salesforce.com/services/oauth2/token",
                        params=payload,
                    )
                else:
                    r = session.post(
                        "https://test.salesforce.com/services/oauth2/token",
                        data=payload,
                        headers=headers,
                    )

                try:
                    r.raise_for_status()
                except Exception as e:
                    self.logger.error(e)
                    self.logger.error(r.json())
                    self.logger.error(f"Failed to authenticate to Salesforce")
                    sys.exit(1)
                json_response = r.json()
            # self.logger.debug(json_response)

            self.sf_token = json_response
            self.access_token = json_response.get("access_token", None)
            self.instance_url = json_response.get("instance_url", None)

    def _get_simple_sf(self, session=None):
        """
        Creates and returns a simple-salesforce object
        """
        from simple_salesforce import Salesforce

        if self.sf_token is None:
            self._authenticate()

        _instance_url = self.get_instance_url()
        _access_token = self.get_access_token()

        _simple_sf = None

        if session is None:
            if self.is_sandbox:
                _simple_sf = Salesforce(
                    instance_url=self.instance_url,
                    session_id=self.access_token,
                    domain="test",
                )
            else:
                _simple_sf = Salesforce(
                    instance_url=self.instance_url, session_id=self.access_token
                )
        else:
            if self.is_sandbox:
                _simple_sf = Salesforce(
                    instance_url=self.instance_url,
                    session_id=self.access_token,
                    domain="test",
                    session=session,
                )
            else:
                _simple_sf = Salesforce(
                    instance_url=self.instance_url,
                    session_id=self.access_token,
                    session=session,
                )

        self.ssf = _simple_sf
        return _simple_sf

    def get_access_token(self):
        """
        Returns access token and instance url for Salesforce instance
        """
        self._authenticate()
        return self.access_token

    def get_instance_url(self):
        """
        Returns access token and instance url for Salesforce instance
        """
        self._authenticate()
        return self.instance_url

    def get_object_by_record_id(self, object_api_name, record_id):
        """Returns DataFrame from Object and Record Id query
        TODO finish dev
        """
        import pandas

        self._get_simple_sf()

        sf_object_result = {}
        if hasattr(self.sf, object_api_name):
            ssf_object = getattr(self.ssf, object_api_name)
        else:
            self.logger.debug(f"Error: object does not exist {object_api_name}")
            sys.exit(1)

        sf_object_result = ssf_object.get(record_id)

        result_dict = {}
        for key, value in sf_object_result.items():
            if key != "attributes":
                result_dict[key] = value
        # del contact['attributes']
        df = pandas.DataFrame(result_dict, index=[0])
        return df

    def get_object_metadata(self, object_api_name):
        """
        Returns dictionary of object metadata
        """
        self._get_simple_sf()
        ssf_object = getattr(self.ssf, object_api_name)
        meta = ssf_object.metadata()
        return meta

    def get_object_description(self, object_api_name):
        """
        Returns dictionary of object metadata
        """
        self._get_simple_sf()
        ssf_object = getattr(self.ssf, object_api_name)
        describe_json = ssf_object.describe()
        return describe_json

    def get_object_fields(self, object_api_name, include_attributes=None):
        fields_json = self.get_object_description(object_api_name).get("fields", None)

        fields = []
        if include_attributes is None:
            return fields_json
        else:
            for field in fields_json:
                field_dict = {}
                for attribute in include_attributes:
                    field_dict[attribute] = field.get(attribute, None)
                fields.append(field_dict)
        return fields

    def get_object_field_picklist(self, object_api_name, field_api_name):
        include_attributes = ["label", "name", "picklistValues"]
        field_json = self.get_object_fields(object_api_name, include_attributes)
        for field in field_json:
            if field["name"] == field_api_name:
                picklist_values = field.get("picklistValues")
                return picklist_values
        return None

    def get_replicatable_objects(self, is_refresh_from_sf=None):
        from collections import OrderedDict
        import json

        self._authenticate()

        if is_refresh_from_sf == True:
            # Get All Salesforce Object Metadata
            sf_dict = OrderedDict()
            sf_dict = self.sf.describe()

            with open("all_objects.json", "w") as f:
                f.write(json.dumps(sf_dict))

        with open("all_objects.json", "r") as read_file:
            loaded_dict = json.loads(read_file.read())
        # self.logger.debug(type(loaded_dict))
        replicateable_object_list = list()
        sobjects_list = loaded_dict.get("sobjects")
        keep_key_list = ["name", "replicateable", "urls"]

        for sobject_dict in sobjects_list:
            if sobject_dict["replicateable"] == True:
                key_filtered_replicateable_sobject_dict = {
                    keep_key: sobject_dict[keep_key] for keep_key in keep_key_list
                }

                # The 'urls' key has a nested dict which we will grab the descirbe and sobject keys from
                key_filtered_replicateable_sobject_dict["url_describe"] = (
                    key_filtered_replicateable_sobject_dict["urls"]["describe"]
                )
                key_filtered_replicateable_sobject_dict["url_sobject"] = (
                    key_filtered_replicateable_sobject_dict["urls"]["sobject"]
                )

                # Delete the urls key after extracting the nested key-values
                del key_filtered_replicateable_sobject_dict["urls"]

                replicateable_object_list.append(
                    key_filtered_replicateable_sobject_dict
                )
            # self.logger.debug(sobject_dict.values())

        self.logger.debug(
            "There are {} replicatable objects in this SF instance".format(
                len(replicateable_object_list)
            )
        )

        # Write JSON to file
        with open("replicateable_object_list.json", "w") as f:
            f.write(json.dumps(replicateable_object_list))

        # Get List of just Object anmes
        with open("replicateable_object_list.json", "r") as read_file:
            sf_list = json.loads(read_file.read())

        with open("replicateable_object_list.txt", "w") as f:
            for sf_object in sf_list:
                self.logger.debug(sf_object["name"])
                f.write(sf_object["name"])
                f.write("\n")

    def get_dataframe_soql(self, soql_query):
        """
        Returns dataframe from SOQL query
        """
        import pandas
        import requests

        self._authenticate()

        self.logger.debug("SOQL Query: {}".format(soql_query))
        with requests.Session() as session:
            ssf = self._get_simple_sf(session)
            sf_result_dict = ssf.query_all(soql_query)

        df = None
        if sf_result_dict is not None and "records" in sf_result_dict:
            df = pandas.DataFrame(sf_result_dict["records"])
            if "attributes" in df.columns:
                df = df.drop(["attributes"], axis=1)

        return df

    def update_record(self, object_api_name, record_id, data_dict):
        import requests

        self._authenticate()
        self.logger.info(
            f'Update - {object_api_name}. Id:{record_id}, data_dict: "{data_dict}"'
        )
        with requests.Session() as session:
            ssf = self._get_simple_sf(session)
            sf_load_object_result = getattr(ssf, object_api_name).update(
                record_id, data_dict
            )
            self.logger.info(f"Load Object Result: {sf_load_object_result}")
        return sf_load_object_result

    def execute_apex(self, operation, method, payload):
        import requests

        self._authenticate()
        with requests.Session() as session:
            ssf = self._get_simple_sf(session)
            self.logger.info(f"Executing Apex: {operation} {method}")
            self.logger.info(f"Payload: {payload}")
            result = ssf.apexecute(method, method=operation, data=payload)
            self.logger.info(f"Response: {result}")
        return result


class SalesforceSOAPClient:
    def __init__(self, conn: dict, logger=None):
        """
        conn = {
            'instance':'sf instance name',
            'user':'sf user',
            'app': 'sf connected app',
        }
        """

        # Handle logger
        self.logger = util_api.get_logger(__name__, "INFO") if not logger else logger
        self.base_ssm_parameter_name = conn.get("base_ssm_parameter_name")

        # Handle connection details
        self.instance = conn.get("instance", None)
        assert self.instance is not None

        self.is_sandbox = True if self.instance != "prod" else False
        self.user = conn.get("user", None)
        self.app = conn.get("app", None)

        assert self.user is not None
        assert self.app is not None

        self.aws_environment = util_api.get_environment_variable(
            logger=self.logger, variable_name="aws_auth_environment"
        )

        self.aws_iam_user = util_api.get_environment_variable(
            logger=self.logger, variable_name="aws_auth_iam_user"
        )

        # assert self.aws_environment is not None
        # assert self.aws_iam_user is not None

        # Do not get access token during class instantiation. Wait for utilization before getting access token.
        self.sf_token = None
        self.access_token = None
        self.instance_url = None
        self.ssf = None

    def _authenticate(self):
        """
        Returns dictionary of OAuth acces_token and instance_url for Salesforce Rest API
        {
            'access_token': 'zzzzz',
            'instance_url': 'https://sf--catalyst.my.salesforce.com',
            'id': 'https://test.salesforce.com/id/00D040000004eqgEAA/0052K00000Aox6CQAR',
            'token_type': 'Bearer', 'issued_at': '1629746368808',
            'signature': 'zzz'
        }
        """
        import pyforce

        if self.sf_token is None:
            try:
                aws_ssm_client = aws_api.SSMClient(
                    environment=self.aws_environment,
                    iam_user=self.aws_iam_user,
                    logger=self.logger,
                )
            except Exception as e:
                self.logger.exception(e)

            sf_username_parameter_name = (
                self.base_ssm_parameter_name
                + self.instance
                + "/"
                + self.user
                + "/username"
            )
            sf_password_parameter_name = (
                self.base_ssm_parameter_name
                + self.instance
                + "/"
                + self.user
                + "/password"
            )
            sf_client_id_parameter_name = (
                self.base_ssm_parameter_name
                + self.instance
                + "/app/"
                + self.app
                + "/client_id"
            )
            sf_client_secret_parameter_name = (
                self.base_ssm_parameter_name
                + self.instance
                + "/app/"
                + self.app
                + "/client_secret"
            )

            sf_username = None
            try:
                sf_username = aws_ssm_client.get_parameter(sf_username_parameter_name)
            except:
                self.logger.error(
                    f"Unable to get SF username from AWS SSM {sf_username_parameter_name}"
                )

            sf_password = None
            try:
                sf_password = aws_ssm_client.get_parameter(
                    sf_password_parameter_name, with_decryption=True
                )
            except:
                self.logger.error(
                    f"Unable to get Salesforce password from AWS SSM {sf_password_parameter_name}"
                )

            sf_client_id = None
            try:
                sf_client_id = aws_ssm_client.get_parameter(sf_client_id_parameter_name)
            except:
                self.logger.error(
                    f"Unable to get SF username from AWS SSM {sf_client_id_parameter_name}"
                )

            sf_client_secret = None
            try:
                sf_client_secret = aws_ssm_client.get_parameter(
                    sf_client_secret_parameter_name, with_decryption=True
                )
            except:
                self.logger.error(
                    f"Unable to get SF username from AWS SSM {sf_client_secret_parameter_name}"
                )

            # Create payload for retrieveing OAuth access token
            payload = {
                "grant_type": "password",
                "client_id": sf_client_id,
                "client_secret": sf_client_secret,
                "username": sf_username,
                "password": sf_password,
            }

            # Login with SOAP
            if self.instance == "prod":
                svc = pyforce.PythonClient(
                    serverUrl="https://login.salesforce.com/services/Soap/u/48.0/"
                )
            else:
                svc = pyforce.PythonClient(
                    serverUrl="https://test.salesforce.com/services/Soap/u/48.0/"
                )

            try:
                svc.login(sf_username, sf_password)
            except Exception as e:
                self.logger.error(f"Failed to authenticate to Salesforce")
                sys.exit(1)

            return svc

    def send_email(self, payload_df):
        # self._authenticate()
        result = self._authenticate().sendEmail(payload_df)
        return result
