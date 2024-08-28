import os
import sys

from cit_pydata.util import api as util_api
from cit_pydata.util import ON_AWS
from cit_pydata.aws import api as aws_api

import time
import numpy as np
import pandas as pd
import polars as pl

from sqlalchemy import dialects, create_engine, text, MetaData, Table
from sqlalchemy.orm import Session


class SQLClient:
    def __init__(self, conn: dict, logger=None):
        """
        SQL Client primarily using SQLAlchemy as it's libray backbone

        conn = {
            'sql_hostname':'',
            'sql_instance:'', #leave key empty for default instance
            'sql_user':'',
            'sql_database':'',
            'dialect':'',
            'execution_options':''
        }
        """

        DEFAULT_DIALECT = "pymssql"
        SUPPORTED_DIALECTS = ["psycopg2", "pymssql", "pyodbc", "pymysql"]

        self.logger = util_api.get_logger(__name__, "INFO") if not logger else logger

        self.base_ssm_parameter_name = conn.get("base_ssm_parameter_name")
        self.sql_hostname = conn.get("sql_hostname", None)
        self.sql_instance = conn.get("sql_instance", None)
        self.sql_user = conn.get("sql_user", None)
        self.sql_database = conn.get("sql_database", None)
        self.dialect = conn.get("dialect", DEFAULT_DIALECT)
        self.execution_options = conn.get("execution_options", None)
        self.ssh_tunnel_host = conn.get("ssh_tunnel_host", None)
        self.ssh_tunnel_port = conn.get("ssh_tunnel_port", None)

        assert self.sql_hostname is not None
        assert self.sql_user is not None
        assert self.sql_database is not None
        assert self.dialect in SUPPORTED_DIALECTS

        self.sql_engine = self._get_sql_engine()

    def _get_sql_engine(self):
        """Returns SQLAlchemy Engine for the sql host, database, and user

        This method will use the AWS IAM credentials defined in the local .env file to get the AWS SSM parameters defining SQL authentication details
        """
        import re
        from urllib.parse import quote

        try:
            _aws_environment = None
            _aws_iam_user = None
            _aws_environment = util_api.get_environment_variable(
                logger=self.logger, variable_name="aws_auth_environment"
            )

            _aws_iam_user = util_api.get_environment_variable(
                logger=self.logger, variable_name="aws_auth_iam_user"
            )

            aws_ssm_client = aws_api.SSMClient(
                environment=_aws_environment, iam_user=_aws_iam_user
            )

        except Exception as e:
            self.logger.exception(e)

        if self.sql_instance:
            sql_password_parameter_name = (
                self.base_ssm_parameter_name
                + self.sql_hostname
                + "/"
                + self.sql_instance
                + "/"
                + self.sql_user
            )
        else:
            sql_password_parameter_name = (
                self.base_ssm_parameter_name + self.sql_hostname + "/" + self.sql_user
            )

        sql_password = None
        try:
            sql_password = aws_ssm_client.get_parameter(
                sql_password_parameter_name, with_decryption=True
            )
        except:
            self.logger.error(
                f"Unable to get SQL User from AWS SSM {sql_password_parameter_name}"
            )

        if sql_password is None:
            self.logger.error(
                f"SQL authentication credential does not exist for server '{self.sql_hostname}' and user '{self.sql_user}'"
            )
            return

        engine_connection_string = None

        if self.dialect == "pyodbc":
            # engine_connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;Trusted_Connection=yes; DATABASE={sql_database}; SERVER={sql_server}; UID={sql_user}; PWD={sql_password}"

            if self.sql_instance:
                engine_connection_string = f"mssql+pyodbc://{self.sql_user}:{sql_password}@{self.sql_hostname}\\{self.sql_instance}/{self.sql_database}?driver=ODBC+Driver+17+for+SQL+Server"
            else:
                engine_connection_string = f"mssql+pyodbc://{self.sql_user}:{sql_password}@{self.sql_hostname}/{self.sql_database}?driver=ODBC+Driver+17+for+SQL+Server"

            sqlalchemy_engine = create_engine(
                engine_connection_string, fast_executemany=True
            )
        elif self.dialect == "psycopg2":
            # self.logger.debug(self.ssh_tunnel_host, self.ssh_tunnel_port)
            if self.ssh_tunnel_host and self.ssh_tunnel_port:
                engine_connection_string = f"postgresql+psycopg2://{self.sql_user}:{quote(sql_password)}@{self.ssh_tunnel_host}:{self.ssh_tunnel_port}/{self.sql_database}"
            else:
                engine_connection_string = f"postgresql+psycopg2://{self.sql_user}:{quote(sql_password)}@{self.sql_hostname}/{self.sql_database}"
            sqlalchemy_engine = create_engine(
                engine_connection_string, executemany_mode="values_plus_batch"
            )
            # sqlalchemy_engine = create_engine(engine_connection_string, pool_pre_ping=True)
        elif self.dialect == "pymysql":
            engine_connection_string = f"mysql+pymysql://{self.sql_user}:{quote(sql_password)}@{self.sql_hostname}/{self.sql_database}"
            sqlalchemy_engine = create_engine(
                engine_connection_string, echo=False, encoding="utf-8"
            )
        else:
            if self.sql_instance:
                engine_connection_string = (
                    "mssql+pymssql://"
                    + self.sql_user
                    + ":"
                    + sql_password
                    + "@"
                    + self.sql_hostname
                    + "\\"
                    + self.sql_instance
                    + "/"
                    + self.sql_database
                )
            else:
                engine_connection_string = (
                    "mssql+pymssql://"
                    + self.sql_user
                    + ":"
                    + sql_password
                    + "@"
                    + self.sql_hostname
                    + "/"
                    + self.sql_database
                )

            if self.execution_options:
                sqlalchemy_engine = create_engine(
                    engine_connection_string,
                    echo=False,
                    execution_options=self.execution_options,
                )
            else:
                sqlalchemy_engine = create_engine(engine_connection_string, echo=False)

        # remove password from string when debugging
        engine_connection_string_public = re.sub(
            re.escape(sql_password), "********", engine_connection_string
        )
        if self.dialect in ("psycopg2", "pymysql"):
            engine_connection_string_public = re.sub(
                re.escape(quote(sql_password)), "********", engine_connection_string
            )
        self.logger.debug(
            f"SQL Engine connection string: {engine_connection_string_public}"
        )
        self.logger.info(
            f"Created SQL Engine '{self.sql_hostname}' and database '{self.sql_database}' with user '{self.sql_user}' and dialect {self.dialect}"
        )

        return sqlalchemy_engine

    def test_connection(self):
        try:
            conn = self.sql_engine.connect()
            conn.close()
            self.logger.info(f"SQL Authentication successful for {self.sql_database}")
            return True
        except Exception as e:
            self.logger.error(
                f"Failed to connect to database: {self.sql_database} with error {str(e)}"
            )
            return False

    def insert_df(
        self, table_name, df, if_exists="replace", chunksize=10**6, **kwargs
    ) -> bool:
        s = time.time()
        status = False
        if chunksize is not None and df.size > chunksize:
            dfs = self.__split_df(df, chunksize)
            status = self.__write_split_df(
                table_name, dfs, if_exists=if_exists, **kwargs
            )
        else:
            status = self.__write_df(table_name, df, if_exists=if_exists, **kwargs)

        if self.logger:
            self.logger.info(
                f"wrote name: {table_name} dataframe shape: {df.shape} within: {round(time.time()-s, 4)}s"
            )
        return status

    def get_dataframe_query(self, query: str, **kwargs) -> pd.DataFrame:
        """Returns Pandas DataFrame from SQL Query
        Arguments:
            query - A SQL Select statement
            kwargs
        """

        if query:
            try:
                result = pd.read_sql_query(query, con=self.sql_engine, **kwargs)
                return result
            except Exception as e:
                self.logger.error(f"Failed to get Dataframe from query: {query}")
                self.logger.error(str(e))
        else:
            self.logger.error(f"Query not supplied")

        return None

    def stream_dataframe_query(self, query: str, kwargs: dict) -> pd.DataFrame:
        """Returns Pandas DataFrame Iterator from SQL Query
        Arguments:
            query - A SQL Select statement
            kwargs
        """

        connection = self.sql_engine.connect().execution_options(stream_results=True)
        if query:
            try:
                for df in pd.read_sql_query(
                    query, con=connection, chunksize=kwargs.get("chunksize")
                ):
                    yield df
            except Exception as e:
                self.logger.error(f"Failed to get Dataframe from query: {query}")
                self.logger.error(str(e))
        else:
            self.logger.error(f"Query not supplied")

        return

    def execute_stored_procedure(
        self, stored_procedure_name, param_list, skip_transaction: bool = False
    ):
        import re

        _param_markers_list = ",".join(["?" for param in param_list])

        if self.dialect == "pymssql":
            try:
                connection = self.sql_engine.raw_connection()
                cursor = connection.cursor()
            except Exception as e:
                self.logger.error(f"Error creating connection and cursor: {str(e)}")
                return False

            try:
                self.logger.debug(
                    f"Executing Stored Procedure {stored_procedure_name} {param_list}"
                )
                # if skip_transaction:
                #     cursor.commit()
                cursor.callproc(stored_procedure_name, param_list)
                cursor.close()
                # connection.commit()
                connection.close()
            except Exception as e:
                self.logger.error(
                    f"Failed to execute stored procedure {stored_procedure_name} {param_list}"
                )
                self.logger.error(f"{e}")
                # self.logger.exception(e)
                return False

        elif self.dialect == "pyodbc":
            try:
                cursor = self.sql_engine.raw_connection().cursor()
                execute_statement = f"{stored_procedure_name} {_param_markers_list}"
                self.logger.debug(f"Executing: {execute_statement} {param_list}")
                if skip_transaction:
                    cursor.commit()
                    cursor.execute(execute_statement, param_list)
                    cursor.commit()
                else:
                    cursor.execute(execute_statement, param_list)
                    cursor.commit()
                self.logger.debug(f"... execution complete.")
            except Exception as e:
                # prevent a rollback by committing before a rollback is attempted
                if skip_transaction:
                    cursor.commit()
                _error_scrubbed = re.sub(
                    r"\(0\); \[[a-z0-9]*\] \[Microsoft\]\[ODBC Driver 17 for SQL Server\]\[SQL Server\]",
                    "\\n",
                    str(e),
                )
                self.logger.error(
                    f"Failed to execute {stored_procedure_name} {param_list}"
                )
                self.logger.error(f"{_error_scrubbed}")
                return False

        return True

    def __write_df(self, table_name, df, **kwargs):
        df.to_sql(table_name, con=self.sql_engine, **kwargs)
        return True

    def __write_split_df(self, table_name, dfs, **kwargs):
        self.__write_df(table_name, dfs[0], **kwargs)
        kwargs.pop("if_exists")
        for df in dfs[1:]:
            self.__write_df(table_name, df, if_exists="append", **kwargs)
        return True

    def __upsert_df(self, insert_statement, df, **kwargs):
        with self.sql_engine.begin() as connection:
            result = connection.execute(insert_statement, df.to_dict("records"))
        return True

    def __upsert_split_df(self, insert_statement, dfs):
        self.__upsert_df(insert_statement, dfs[0])
        for df in dfs[1:]:
            self.__upsert_df(insert_statement, df)
        return True

    def __split_df(self, df, chunksize):
        from math import ceil

        chunk_count = int(ceil(df.size / chunksize))
        return np.array_split(df, chunk_count)

    def execute_sql(self, sql_statement, return_type="dataframe"):
        dataframe = None
        return_obj = None

        if self.dialect == "pymssql":
            self.logger.info(f'Executing SQL "{sql_statement}"')
            with Session(self.sql_engine) as session:
                session.execute(sql_statement)
                session.commit()

        elif self.dialect == "pyodbc":
            cursor = self.sql_engine.raw_connection().cursor()
            self.logger.info(f"Executing SQL '{sql_statement}'")

            try:
                cursor.execute(sql_statement)
                rows = cursor.fetchall()
                if rows:
                    dataframe = pd.DataFrame.from_records(
                        rows, columns=[col[0] for col in cursor.description]
                    )
            except Exception as e:
                self.logger.error(f"Failed to execute SQL {str(e)}")

            cursor.commit()
        elif self.dialect == "psycopg2":
            self.logger.info(f"Executing SQL '{sql_statement}'")
            try:
                self.sql_engine.execute(
                    text(sql_statement).execution_options(autocommit=True)
                )
            except Exception as e:
                self.logger.error(f"Failed to execute SQL {str(e)}")

        if return_type == "dataframe":
            return_obj = dataframe
        else:
            return_obj = True

        return return_obj

    def execute2(self, sql_statement: str, statement_type: str = "query"):

        if self.dialect == "pymssql":
            if statement_type == "query":
                self.logger.info(f'Executing SQL "{sql_statement}"')
            else:
                self.logger.info(f'Executing SQL "{statement_type}"')
            try:
                with Session(self.sql_engine) as session:
                    session.execute(sql_statement)
                    session.commit()
            except Exception as e:
                return False

        elif self.dialect == "pyodbc":
            cursor = self.sql_engine.raw_connection().cursor()
            if statement_type == "query":
                self.logger.info(f"Executing SQL '{sql_statement}'")
            else:
                self.logger.info(f'Executing SQL "{statement_type}"')
            try:
                cursor.execute(sql_statement)
            except Exception as e:
                self.logger.error(f"Failed to execute SQL {str(e)}")
                return False
            cursor.commit()
        elif self.dialect == "psycopg2":
            self.logger.info(f"Executing SQL '{sql_statement}'")
            try:
                self.sql_engine.execute(
                    text(sql_statement).execution_options(autocommit=True)
                )
            except Exception as e:
                self.logger.error(f"Failed to execute SQL {str(e)}")
                return False
        return True

    def execute_sql_select(self, sql_statement):
        dataframe = None
        with self.sql_engine.connect() as connection:
            cursor_result = connection.execute(text(sql_statement))
            if cursor_result:
                dataframe = pd.DataFrame.from_records(
                    cursor_result, columns=[col[0] for col in cursor_result.keys()]
                )

        return dataframe

    def execute_sql_select2(self, sql_statement):
        dataframe = None
        with self.sql_engine.connect() as connection:
            cursor_result = connection.execute(text(sql_statement))
            if cursor_result:
                dataframe = pd.DataFrame.from_records(
                    cursor_result, columns=cursor_result.keys()
                )

        return dataframe

    def execute_sql_select_pl(self, sql_statement, execute_options: dict = None) -> pl.DataFrame:
        """ similar to execute_sql_select2,
            but the function process the sql with polars and return a polars DataFrame"""
            
        dataframe = None
        if not execute_options:
            execute_options = {}

        with self.sql_engine.connect() as connection:
            dataframe = pl.read_database(
                query=text(sql_statement),
                connection=connection,
                execute_options=execute_options,
            )

        return dataframe

    def get_dataframe_table(self, table_name, chunk_count=None, **kwargs):
        from itertools import islice

        s = time.time()
        if "chunksize" not in kwargs.keys():
            kwargs["chunksize"] = 10**6

        dataframes = pd.read_sql_table(table_name, self.sql_engine, **kwargs)

        try:
            dataframe = pd.concat(islice(dataframes, chunk_count), axis=0)
        except (
            ValueError
        ):  # No objects to concetenate. dfs is a generator object so has no len() property!
            if self.logger:
                self.logger.warning(
                    "No objects to concetenate on table_name: {}".format(table_name)
                )
            return None

        if self.logger:
            self.logger.info(
                "fetched name: {} dataframe shape: {} within: {}".format(
                    table_name, dataframe.shape, round(time.time() - s, 4)
                )
            )
        return dataframe

    def create_csv_from_query(self, query, folder_path, file_name):
        df = self.get_dataframe_query(query)
        if not os.path.exists(folder_path):
            self.logger.debug(
                f"folder {folder_path} does not exist. creating folder..."
            )
            os.makedirs(folder_path)
        _csv_target_file = os.path.join(folder_path, file_name)
        self.logger.debug(f"Creating CSV - {_csv_target_file}")
        df.to_csv(_csv_target_file, index=False)
        self.logger.debug(f"Created CSV - {_csv_target_file}")

    def truncate_table(self, table_name):
        sql_statement = f"TRUNCATE TABLE {table_name}"
        self.execute_sql(sql_statement)

    # def get_table_count(self, table_name):
    #     sql_statement = f'SELECT count(*) as count FROM {table_name}'
    #     dataframe = self.execute_sql_select(self.sql_engine, sql_statement)
    #     count = int(dataframe.iloc[0,0])
    #     if selflogger:
    #         logger.info(f'Count of table {table_name} is {count}')
    #     return count

    def upsert_df(
        self,
        table_name,
        df,
        chunksize=10**6,
        table_schema=None,
        upsert_kwargs: dict = None,
    ):
        """
        upsert_kwargs = {
            'upsert_id_index_elements':'',
            'exclude_columns_on_conflict_update':''
        }
        """

        if self.dialect != "psycopg2":
            self.logger.error(f"unsupported dialect for upsert {self.dialect}")

        assert type(df) == pd.DataFrame
        metadata = MetaData(bind=self.sql_engine)
        if table_schema:
            target_table = Table(
                table_name, metadata, schema=table_schema, autoload=True
            )
        else:
            target_table = Table(table_name, metadata, autoload=True)

        upsert_id_index_elements = upsert_kwargs.get("upsert_id_index_elements", None)

        # exlude these columns from update on insert conflict
        exclude_columns_on_conflict_update = upsert_kwargs.get(
            "exclude_columns_on_conflict_update", None
        )

        # build insert statement
        insert_statement = dialects.postgresql.insert(target_table)

        on_update_dict = {}

        # get columns for insert conflict update
        for key, value in insert_statement.excluded.items():
            if key not in exclude_columns_on_conflict_update:
                on_update_dict[key] = value

        if not on_update_dict:
            self.logger.error(
                f"Upsert not configured correctly. No columns provided for updating"
            )
        # self.logger.debug(on_update_dict)
        # build upsert statement for postgresql.
        on_conflict_statement = insert_statement.on_conflict_do_update(
            index_elements=upsert_id_index_elements, set_=on_update_dict
        )

        s = time.time()
        status = False

        if chunksize is not None and df.size > chunksize:
            dfs = self.__split_df(df, chunksize)
            status = self.__upsert_split_df(on_conflict_statement, dfs)
        else:
            status = self.__upsert_df(on_conflict_statement, df)

        if self.logger:
            self.logger.info(
                f"Upserted dataframe to table {table_name} with shape: {df.shape} within: { round(time.time() - s, 4)}s"
            )
        return status

    def upsert_df_2(
        self,
        table_name,
        df,
        chunksize=10**6,
        table_schema=None,
        upsert_kwargs: dict = None,
    ):
        """
        upsert_kwargs = {
            'upsert_id_index_elements':'',
            'exclude_columns_on_conflict_update':''
        }
        """

        if self.dialect != "psycopg2":
            self.logger.error(f"unsupported dialect for upsert {self.dialect}")

        assert type(df) == pd.DataFrame
        metadata = MetaData(bind=self.sql_engine)
        if table_schema:
            target_table = Table(
                table_name, metadata, schema=table_schema, autoload=True
            )
        else:
            target_table = Table(table_name, metadata, autoload=True)

        upsert_id_index_elements = upsert_kwargs.get("upsert_id_index_elements", None)

        # exlude these columns from update on insert conflict
        exclude_columns_on_conflict_update = upsert_kwargs.get(
            "exclude_columns_on_conflict_update", []
        )
        on_conflict = upsert_kwargs.get("on_conflict", "do_nothing")
        # build insert statement
        insert_statement = dialects.postgresql.insert(target_table)
        columns_to_update = set(df.columns)
        on_update_dict = {}
        if on_conflict == "do_nothing":
            on_conflict_statement = insert_statement.on_conflict_do_nothing(
                index_elements=upsert_id_index_elements
            )
        elif on_conflict == "do_update":
            # get columns for insert conflict update
            for key, value in insert_statement.excluded.items():
                if (
                    key in columns_to_update
                    and key not in exclude_columns_on_conflict_update
                ):
                    on_update_dict[key] = value

            if not on_update_dict:
                self.logger.error(
                    f"Upsert not configured correctly. No columns provided for updating"
                )
            # self.logger.debug(on_update_dict)
            # build upsert statement for postgresql.
            # self.logger.info(on_update_dict.keys())
            on_conflict_statement = insert_statement.on_conflict_do_update(
                index_elements=upsert_id_index_elements, set_=on_update_dict
            )
        else:
            self.logger.error(
                f"on_conflict kwarg not implemented. Please choose: do_nothing or do_update instead"
            )
        s = time.time()
        status = False

        if chunksize is not None and df.size > chunksize:
            dfs = self.__split_df(df, chunksize)
            status = self.__upsert_split_df(on_conflict_statement, dfs)
        else:
            status = self.__upsert_df(on_conflict_statement, df)

        if self.logger:
            self.logger.info(
                f"Upserted dataframe to table {table_name} with shape: {df.shape} within: { round(time.time() - s, 4)}s"
            )
        return status

    def _psql_insert_copy(self, table, conn, keys, data_iter):
        """
        Execute SQL statement inserting data

            Parameters
            ----------
            table : pandas.io.sql.SQLTable
            conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
            keys : list of str
                Column names
            data_iter : Iterable that iterates the values to be inserted
        """
        # gets a DBAPI connection that can provide a cursor
        import csv
        from io import StringIO

        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(
                s_buf,
                delimiter="\x1e",
                quoting=csv.QUOTE_NONE,
                quotechar="",
                escapechar="\\",
                doublequote=False,
            )
            writer.writerows(data_iter)
            s_buf.seek(0)

            columns = ", ".join('"{}"'.format(k) for k in keys if str(k).lower())
            if table.schema:
                table_name = '{}."{}"'.format(table.schema, table.name)
            else:
                table_name = f'"{table.name}"'

            sql = f"COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\x1e')"
            cur.copy_expert(sql=sql, file=s_buf)

    #### HAS NOT BEEN TESTED YET  MAY NOT WORK####
    def postgres_quickexport_df(self, table_name):
        from io import StringIO

        if self.dialect != "psycopg2":
            self.logger.error(f"unsupported dialect for upsert {self.dialect}")
        dbapi_conn = self._get_sql_engine.connect()
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            sql = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(table_name)
            cur.copy_expert(sql=sql, file=s_buf)
            df = pd.read_csv(s_buf)
        return df

    def postgres_quickinsert_df(
        self,
        table_name,
        df,
        index,
        schema="public",
        on_conflict="DO NOTHING",
        chunksize=None,
        thread_id=0,
    ):
        # Increases load speed to postgres for large tables
        on_conflict = on_conflict.replace("_", " ").upper()
        if self.dialect != "psycopg2":
            self.logger.error(f"unsupported dialect for upsert {self.dialect}")
        assert type(df) == pd.DataFrame
        if on_conflict.upper() not in ("DO NOTHING", "DO UPDATE"):
            raise ValueError("on_conflict must be: 'DO NOTHING' or 'DO UPDATE'")
        if df.empty:
            self.logger.info("Empty Dataframe. Ignoring...")
            return
        s = time.time()
        metadata = MetaData(bind=self.sql_engine)
        if schema:
            target_table = Table(table_name, metadata, schema=schema, autoload=True)
        else:
            target_table = Table(table_name, metadata, autoload=True)

        renamed_cols = {
            col1: col2
            for col2 in list(target_table.columns.keys())
            for col1 in list(df.columns)
            if str(col1) != str(col2) and str(col1).lower() == str(col2).lower()
        }
        str_cols = [
            col2.name
            for col2 in target_table.columns
            for col1 in list(df.columns)
            if str(col1).lower() == str(col2.name).lower()
            and col2.type.python_type == str
        ]
        int_cols = [
            col2.name
            for col2 in target_table.columns
            for col1 in list(df.columns)
            if str(col1).lower() == str(col2.name).lower()
            and col2.type.python_type == int
        ]

        df = df.rename(columns=renamed_cols)
        df[str_cols] = df[str_cols].replace({"": '""'})
        my_to_int = np.frompyfunc(self.to_int, 1, 1)
        df[int_cols] = df[int_cols].apply(my_to_int)
        column_names = ",".join([f'"{col}"' for col in list(df.columns)])
        time_now = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time()))
        tmp_table_name = (
            f"quickinsert_{table_name}_{time_now}_{os.getpid()}_{thread_id}"
        )
        schema_tmp_table_name = f'{schema}."{tmp_table_name}"'
        schema_table_name = f'{schema}."{table_name}"'
        create_temp_table_sql = f"""
            DROP TABLE IF EXISTS {schema_tmp_table_name};
            CREATE TABLE {schema_tmp_table_name}
            (LIKE {schema_table_name} INCLUDING DEFAULTS)
        """
        insert_temp_table_to_main_sql = f"""
            INSERT INTO {schema_table_name}({column_names})
            SELECT {column_names}
            FROM {schema_tmp_table_name}
            ON CONFLICT {on_conflict};
        """
        cleanup_temp_tabl_sql = f"""
            DROP TABLE IF EXISTS {schema_tmp_table_name};
        """
        try:
            if self.logger:
                self.logger.debug(f"Creating temp table on {schema_tmp_table_name}...")
            self.sql_engine.execute(create_temp_table_sql)
            if self.logger:
                self.logger.debug(f"{schema_tmp_table_name} created successfully.")
            if self.logger:
                self.logger.debug(
                    f"Loading using FROM CSV to {schema_tmp_table_name}..."
                )

            df.to_sql(
                name=tmp_table_name,
                schema=schema,
                con=self.sql_engine,
                index_label=index,
                chunksize=chunksize,
                if_exists="append",
                index=False,
                method=self._psql_insert_copy,
            )
            if self.logger:
                self.logger.debug(f"Successfully loaded to {schema_tmp_table_name}.")
                self.logger.debug(
                    f"Merging {schema_tmp_table_name} into {schema_table_name}. Cleaning up..."
                )
            self.sql_engine.execute(insert_temp_table_to_main_sql)
            self.sql_engine.execute(cleanup_temp_tabl_sql)
            if self.logger:
                diff = time.time() - s
                self.logger.info(
                    f"Inserted dataframe to table {table_name} with shape: {df.shape}."
                )
                self.logger.info(
                    f"Insert duration: {diff//3600:01.0f}:{(diff%3600)//60:02.0f}:{float(diff%60):02.6f}"
                )
            return True
        except Exception as e:
            if self.logger:
                self.logger.error(e)
                self.logger.error(
                    "Error loading data Please make sure reserved char | is not in the data. Cleaning up..."
                )
            self.sql_engine.execute(cleanup_temp_tabl_sql)
            raise e
            # return False

    def to_int(self, x):
        return f"{x:.0f}" if x is not None else ""

    def describe_database(self):
        metadata = MetaData(bind=self.sql_engine)
        metadata.reflect(self.sql_engine)
        if self.logger:
            self.logger.info(metadata.tables.keys())
        else:
            self.logger.debug(metadata.tables.keys())

    # This function fixes int cols in a dataframe that have been converted to float due to NULL
    def df_fix_int_cols(self, table_name, df, schema="public"):

        metadata = MetaData(bind=self.sql_engine)
        if schema:
            table = Table(table_name, metadata, schema=schema, autoload=True)
        else:
            table = Table(table_name, metadata, autoload=True)

        int_cols = [
            col2.name
            for col2 in table.columns
            for col1 in list(df.columns)
            if str(col1).lower() == str(col2.name).lower()
            and col2.type.python_type == int
        ]

        my_to_int = np.frompyfunc(self.to_int, 1, 1)
        df[int_cols] = df[int_cols].apply(my_to_int)

        return df

    # Execute SQL statement with timeout and only returns once the entire execution is complete - Function commits all transactions
    def execute_with_timeout(
        self, sql_statement: str, timeout: int, statement_type: str = "query"
    ):
        # Section not tested
        if self.dialect == "pymssql":
            if statement_type == "query":
                self.logger.info(f'Executing SQL "{sql_statement}"')
            else:
                self.logger.info(f'Executing SQL "{statement_type}"')
            try:
                with Session(self.sql_engine) as session:
                    cursor = session.execute(sql_statement)
                    slept = 0
                    while cursor.nextset():
                        if slept >= timeout:
                            self.logger.error(
                                f"Timeout threshold exceeded. The timeout period elapsed prior to completion of the operation."
                            )
                            return False
                        time.sleep(1)
                        slept += 1
                    session.commit()
            except Exception as e:
                self.logger.error(f"Failure during SQL execution {str(e)}")
                return False

        # Section tested
        elif self.dialect == "pyodbc":
            cursor = self.sql_engine.raw_connection().cursor()
            if statement_type == "query":
                self.logger.info(f"Executing SQL '{sql_statement}'")
            else:
                self.logger.info(f'Executing SQL "{statement_type}"')
            try:
                cursor = cursor.execute(sql_statement)
                slept = 0
                while cursor.nextset():
                    if slept >= timeout:
                        self.logger.error(
                            f"Failed to execute SQL {sql_statement}. Execution lasted longer than time threshold."
                        )
                        return False
                    time.sleep(1)
                    slept += 1
            except Exception as e:
                self.logger.error(f"Failed to execute SQL {str(e)}")
                # 12/06/2023
                # Added commit statement to ensure transactions are committed independently of whether there's an exception or not
                # This is needed to trace back errors in SFSync log tables when refreshing objects/ object groups
                # Previously, in case of an exception/ error during the refresh, there were no logs in SFSync log tables because of uncommitted transaction
                cursor.commit()
                return False
            cursor.commit()

        # Section not tested
        elif self.dialect == "psycopg2":
            self.logger.info(f"Executing SQL '{sql_statement}'")
            try:
                cursor = self.sql_engine.execute(
                    text(sql_statement).execution_options(autocommit=True)
                )
                slept = 0
                while cursor.nextset():
                    if slept >= timeout:
                        self.logger.error(
                            f"Failed to execute SQL {sql_statement}. Execution lasted longer than time threshold."
                        )
                        return False
                    time.sleep(1)
                    slept += 1
            except Exception as e:
                self.logger.error(f"Failed to execute SQL {str(e)}")
                return False
        return True
