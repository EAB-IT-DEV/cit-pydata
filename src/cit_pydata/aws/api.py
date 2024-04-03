import sys

from cit_pydata.util import api as util_api
from cit_pydata.util import ON_AWS

from typing import Literal
import boto3


def _get_boto_session(
    service: str = None,
    environment: str = None,
    iam_user: str = None,
    path_to_env_file: str = None,
    region: str = None,
    logger=util_api.get_logger(__name__, "info"),
):
    if ON_AWS:
        return boto3.Session()

    _environment_variable = environment + "_" + iam_user + "_" + "aws_access_key_id"
    aws_access_key_id = util_api.get_environment_variable(
        logger=logger,
        path_to_env_file=path_to_env_file,
        variable_name=_environment_variable,
    )

    if aws_access_key_id is None:
        logger.error(f"Access Key Id does not exist for environment {environment}")
        return

    _environment_variable = environment + "_" + iam_user + "_" + "aws_secret_access_key"
    aws_secret_access_key = util_api.get_environment_variable(
        logger=logger,
        path_to_env_file=path_to_env_file,
        variable_name=_environment_variable,
    )

    if aws_secret_access_key is None:
        logger.error(f"Secret Key not specified for environment {environment}")
        return

    _aws_region = region
    if _aws_region is None:
        _environment_variable = environment + "_" + iam_user + "_" + "aws_region"
        _aws_region = util_api.get_environment_variable(
            logger=logger,
            path_to_env_file=path_to_env_file,
            variable_name=_environment_variable,
        )

    if _aws_region is None:
        logger.error(f"AWS Region not specified for environment {environment}")
        return

    _session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=_aws_region,
    )

    return _session


class S3Client:
    def __init__(
        self,
        environment: str = None,
        iam_user: str = None,
        path_to_env_file: str = None,
        log_level: Literal["info", "debug"] = "info",
    ):
        self.logger = util_api.get_logger(__name__, log_level)
        environment = environment.lower()

        self.session = _get_boto_session(
            service="s3",
            environment=environment,
            iam_user=iam_user,
            path_to_env_file=path_to_env_file,
            logger=self.logger,
        )
        self.client = self.session.client("s3")

    def get_object_metadata(self, **kwargs):
        """
        Returns pandas dataframe with S3 Objects

        kwargs for method client.list_objects_v2
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
        """
        import pandas

        # self.logger.info(f'Getting S3 objects: {**kwargs}')
        object_list = []
        next_page_token = "init"

        # Object metadata is retrieved in batches of 1000
        while next_page_token:
            response: dict = None
            if next_page_token == "init":
                response = self.client.list_objects_v2(**kwargs)
            else:
                response = self.client.list_objects_v2(
                    ContinuationToken=next_page_token, **kwargs
                )

            next_page_token = response.get("NextContinuationToken", None)
            # self.logger.debug(next_page_token)
            for object_item in response["Contents"]:
                object_list.append(object_item)
            response.pop("Contents")

        df_s3_objects = pandas.DataFrame(object_list)
        # self.logger.debug(response)
        # self.logger.debug(df_s3_objects.info())

        return response, df_s3_objects

    def metadata_to_sql(self, sql_client, table_name: str, dataframe):
        import sqlalchemy

        try:
            sql_client.insert_df(
                table_name=table_name,
                df=dataframe,
                index=False,
                dtype={"LastModified": sqlalchemy.DateTime},
            )
            # dataframe.to_sql(name = table_name, con = sql_client.sql_engine, if_exists='replace', index=False, dtype={'LastModified':sqlalchemy.DateTime})
        except Exception as e:
            self.logger.error(f"Failed to send S3 Object metadata to SQL")
            self.logger.exception(str(e))


class SSMClient:
    def __init__(
        self,
        environment: str = None,
        iam_user: str = None,
        path_to_env_file: str = None,
        log_level: Literal["info", "debug"] = "info",
        logger=None,
    ):
        self.logger = util_api.get_logger(__name__, log_level) if not logger else logger
        self.session = _get_boto_session(
            service="ssm",
            environment=environment,
            iam_user=iam_user,
            path_to_env_file=path_to_env_file,
            logger=self.logger,
        )
        self.client = self.session.client("ssm")

    def get_parameter(
        self, name: str, with_decryption: bool = False, is_verbose: bool = False
    ):
        value = None
        try:
            value = self.client.get_parameter(Name=name, WithDecryption=with_decryption)
        except Exception as ce:
            self.logger.error(ce)
            return None

        if value is None:
            self.logger.error(f'SSM Parameter not found "{name}"')
            return

        if is_verbose:
            return value
        else:
            return value["Parameter"]["Value"]

        # for parameter in response['Parameters']:
        #     return parameter['Value']

    def describe_parameters(
        self, filter_parameters: list[dict], is_verbose: bool = False
    ):
        """
        filter_parameters=[
        {
            'Key': 'string',
            'Option': 'string',
            'Values': [
                'string',
            ]
        },
        """
        max_results = 50
        describe_param_dict: dict = self.client.describe_parameters(
            ParameterFilters=filter_parameters, MaxResults=max_results
        )

        param_list = describe_param_dict.get("Parameters", None)
        # return_list = []
        # for parameter in param_list
        #     param_keys_to_return_list = ['Name']
        #     param_dict = parameter.get()
        #     return_list.append()

        return param_list
