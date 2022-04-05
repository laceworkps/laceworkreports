"""
Example script showing how to use the LaceworkClient class.
"""

import base64
import csv
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path

import jinja2
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy_utils.functions import create_database, database_exists

from laceworkreports import common

from .DataHelpers import DataHelpers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)s - %(message)s",
)

load_dotenv()

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
LQL_PAGINATION_MAX = 5000
MAX_PSQL_COLUMN_NAME_LENGTH = 63


class DataHandlerTypes(Enum):
    POSTGRES = "postgres"
    SQLITE = "sqlite"
    PANDAS = "pandas"
    DICT = "dict"
    CSV = "csv"
    JSON = "json"
    JINJA2 = "jinja2"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class DataHandlerCliTypes(Enum):
    POSTGRES = "postgres"
    SQLITE = "sqlite"
    CSV = "csv"
    JSON = "json"
    JINJA2 = "jinja2"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class APIv2Helper:
    def __init__(self, session, object_type, endpoint_root="/api/v2"):
        self.session = session
        self._session = session
        self._object_type = object_type
        self._endpoint_root = endpoint_root

    def build_url(self, id=None, resource=None, action=None):
        """
        Builds the URL to use based on the endpoint path, resource, type, and ID.
        :param id: A string representing the ID of an object to use in the URL
        :param resource: A string representing the type of resource to append to the URL
        :param action: A string representing the type of action to append to the URL
        """

        result = f"{self._endpoint_root}/{self._object_type}"

        if resource:
            result += f"/{resource}"
        if action:
            result += f"/{action}"
        if id:
            result += f"/{id}"

        return result

    def search(self, json=None, resource=None, **kwargs):
        response = self._session.post(
            self.build_url(resource=resource, action="search"), json=json
        )
        while True:
            response_json = response.json()
            yield response_json

            try:
                next_page = (
                    response_json.get("paging", {}).get("urls", {}).get("nextPage")
                )
            except Exception:
                next_page = None

            if next_page:
                response = self._session.get(next_page, **kwargs)
            else:
                break


class DataHandler:
    def __init__(
        self,
        format,
        file_path="export.csv",
        template_path=None,
        append=False,
        flatten_json=False,
        dtypes=None,
        db_connection=None,
        db_table="export",
        db_if_exists="replace",
        db_create_if_missing=True,
        sample=False,
    ):
        self.format = format

        # check for valid format type
        if not DataHandlerTypes.has_value(self.format.value):
            raise ValueError(
                "Unsupported export format, exepcted {} found: {}".format(
                    list(DataHandlerTypes), self.format
                )
            )
        else:
            logging.info(f"Processing data with export format: {self.format}")

        # check required args
        if self.format in [DataHandlerTypes.CSV, DataHandlerCliTypes.CSV]:
            if file_path is None:
                raise ValueError(f"file_path required for {self.format} type")
        elif self.format in [DataHandlerTypes.JSON, DataHandlerCliTypes.JSON]:
            if file_path is None:
                raise ValueError(f"file_path required for {self.format} type")
        elif self.format in [DataHandlerTypes.JINJA2, DataHandlerCliTypes.JINJA2]:
            if file_path is None:
                raise ValueError(f"file_path required for {self.format} type")
            if template_path is None:
                raise ValueError(f"template_path required for {self.format} type")
        elif self.format in [DataHandlerTypes.POSTGRES, DataHandlerCliTypes.POSTGRES]:
            if db_connection is None:
                raise ValueError(f"db_connection required for {self.format} type")
            if db_table is None:
                raise ValueError(f"db_table required for {self.format} type")
        elif self.format in [DataHandlerTypes.SQLITE, DataHandlerCliTypes.SQLITE]:
            if db_connection is None:
                raise ValueError(f"db_connection required for {self.format} type")
            if db_table is None:
                raise ValueError(f"db_table required for {self.format} type")

        self.dataset = []
        self.reader = None
        self.file_path = file_path
        self.template_path = template_path
        self.db_connection = db_connection
        self.db_table = db_table
        self.db_if_exists = db_if_exists
        self.db_create_if_missing = db_create_if_missing
        self.dropped_columns = {}
        self.flatten_json = flatten_json
        self.append = append
        self.sample = sample

        # dtypes is the override for sql data types - empty when not provided
        if dtypes is None:
            self.dtypes = {}
        else:
            self.dtypes = dtypes

    def __open(self):
        if self.format in [DataHandlerTypes.CSV, DataHandlerCliTypes.CSV]:
            self.header = False
            self.path = Path(self.file_path)

            # check if we append to existing file or remove
            if Path(self.file_path).exists and not self.append:
                logging.info(f"Removing existing file - append={self.append}")
                self.path.unlink(missing_ok=True)

            if self.append:
                self.fp = open(self.file_path, "a")
            else:
                self.fp = open(self.file_path, "w")

            self.writer = csv.writer(self.fp, quoting=csv.QUOTE_ALL)
        elif self.format in [DataHandlerTypes.JSON, DataHandlerCliTypes.JSON]:
            self.header = False
            self.path = Path(self.file_path)

            # check if we append to existing file or remove
            if Path(self.file_path).exists and not self.append:
                logging.info(f"Removing existing file - append={self.append}")
                self.path.unlink(missing_ok=True)

            if self.append:
                self.fp = open(self.file_path, "a")
            else:
                self.fp = open(self.file_path, "w")

        elif self.format in [DataHandlerTypes.POSTGRES, DataHandlerCliTypes.POSTGRES]:
            try:
                self.db_engine = create_engine(self.db_connection)
                logging.info(
                    f'Connecting to "{self.db_engine.url.database}" on port {self.db_engine.url.port} as user "{self.db_engine.url.username}"'
                )
                self.db_engine.connect()
            except sqlalchemy.exc.OperationalError as e:
                if re.match("password authentication failed for user", str(e)):
                    logging.critical(str(e))
                    exit(1)
                elif re.search(r' database "[^"]+" does not exist', str(e)):
                    # check for the db if it doesn't exist create it
                    if (
                        not database_exists(self.db_connection)
                        and self.db_create_if_missing
                    ):
                        logging.info(
                            f"Creating database - db_create_if_missing=True: {self.db_engine.url.database}"
                        )
                        create_database(self.db_engine.url)
                    else:
                        logging.critical(str(e))

                elif re.search(
                    r'could not translate host name "[^"]+" to address: nodename nor servname provided, or not known',
                    str(e),
                ):
                    logging.critical(str(e))
                    raise Exception(str(e))
                elif re.search(
                    r'connection to server at "[^"]+", port \d+ failed: Operation timed out',
                    str(e),
                ):
                    logging.critical(str(e))
                    raise Exception(str(e))

            # connect to the database
            self.conn = self.db_engine.connect()
            metadata = MetaData(bind=self.conn)

            # if we are replacing drop the table first
            if self.db_if_exists == common.DBInsertTypes.Replace.value:
                t = Table(self.db_table, metadata)
                t.drop(self.conn, checkfirst=True)
            elif (
                self.db_if_exists == common.DBInsertTypes.Fail
                and self.db_engine.has_table(self.db_table)
            ):
                logging.error("Table already exists and db_if_exists=fail")
                raise Exception("Table already exists and db_if_exists=fail")
            elif (
                self.db_if_exists == common.DBInsertTypes.Append.value
                and not self.db_engine.has_table(self.db_table)
            ):
                logging.warning(
                    "Database table does not exist and db_if_exists=replace: Table will be created"
                )
        elif self.format in [DataHandlerTypes.SQLITE, DataHandlerCliTypes.SQLITE]:

            # connect to the db
            logging.info(f"Connecting: {self.db_connection}")
            self.db_engine = create_engine(self.db_connection, echo=False)

            # if db doesn't exist create it
            if not database_exists(self.db_engine.url):
                create_database(self.db_engine.url)

            # connect to the database
            self.conn = self.db_engine.connect()

            # drop table if it exists
            metadata = MetaData(bind=self.conn)

            # if we are replacing drop the table first
            if self.db_if_exists == common.DBInsertTypes.Replace:
                t = Table(self.db_table, metadata)
                t.drop(self.conn, checkfirst=True)
            elif (
                self.db_if_exists == common.DBInsertTypes.Fail
                and self.db_engine.has_table(self.db_table)
            ):
                logging.error("Table already exists and db_if_exists=fail")
                raise Exception("Table already exists and db_if_exists=fail")
            elif (
                self.db_if_exists == common.DBInsertTypes.Append
                and not self.db_engine.has_table(self.db_table)
            ):
                logging.warning(
                    "Database table does not exist and db_if_exists=replace: Table will be created"
                )

    def __close(self):
        if self.format in [DataHandlerTypes.CSV, DataHandlerCliTypes.CSV]:
            self.fp.close()

        # for jinja2 we have aggregated into a dict, pass that to the template
        elif self.format in [DataHandlerTypes.JINJA2, DataHandlerCliTypes.JINJA2]:
            report_template = Path(self.template_path).resolve()
            fileloader = jinja2.FileSystemLoader(
                searchpath=os.path.dirname(report_template)
            )
            env = jinja2.Environment(
                loader=fileloader, extensions=["jinja2.ext.do"], autoescape=True
            )
            template = env.get_template(os.path.basename(report_template))
            result = template.render(
                datasets=self.dataset,
                rows=len(self.dataset),
                datetime=datetime,
                timedelta=timedelta,
                config=common.config,
                pandas=pd,
                base64=base64,
            )

            Path(self.file_path).write_text(result)

    def insert(self, row):
        # only flatten json if we're not dumping json
        if self.flatten_json and self.format not in [
            DataHandlerTypes.JSON,
            DataHandlerCliTypes.JSON,
        ]:
            row = DataHelpers.dict_flatten(row)

        if self.format in [DataHandlerTypes.CSV, DataHandlerCliTypes.CSV]:
            if not self.header:
                self.writer.writerow(row.keys())
                self.header = True
            self.writer.writerow(row.values())
        elif self.format in [DataHandlerTypes.JSON, DataHandlerCliTypes.JSON]:
            self.fp.write(f"{json.dumps(row)}\n")
        # if we're doing jinja2 formatting aggregate the result in a dict
        elif self.format in [
            DataHandlerTypes.DICT,
            DataHandlerTypes.JINJA2,
            DataHandlerCliTypes.JINJA2,
        ]:
            self.dataset.append(row)
        elif self.format == DataHandlerTypes.PANDAS:
            if not isinstance(self.dataset, pd.DataFrame):
                self.dataset = pd.DataFrame([row])
            else:
                df = pd.DataFrame([row])
                self.dataset = pd.concat([self.dataset, df], ignore_index=True)
        elif self.format in [DataHandlerTypes.POSTGRES, DataHandlerCliTypes.POSTGRES]:
            try:
                # determine special column handling for json data
                if not self.dtypes:
                    dtypes = {}
                    for k in row.keys():
                        if isinstance(row[k], dict) or isinstance(row[k], list):
                            dtypes[k] = sqlalchemy.types.JSON
                else:
                    dtypes = self.dtypes

                df = pd.DataFrame([row])

                # check for column names that are over max (result of json flatten)
                long_col = [
                    x for x in df.columns if len(x) > MAX_PSQL_COLUMN_NAME_LENGTH
                ]

                long_col_count = len(long_col)
                if long_col_count > 0:
                    # track all dropped column names
                    logging.warning(
                        f"Column {long_col} name length greater than {MAX_PSQL_COLUMN_NAME_LENGTH}, dropping column"
                    )
                    # self.dropped_columns.updated(long_col)
                    df.drop(columns=long_col, inplace=True)

                df.to_sql(
                    self.db_table,
                    if_exists=common.DBInsertTypes.Append.value,
                    index=False,
                    con=self.conn,
                    dtype=dtypes,
                )
            except sqlalchemy.exc.ProgrammingError as e:
                logging.error(e)
                # ensure that any additional columns are added as needed
                for column in df.columns:
                    rows = self.conn.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns WHERE table_name=:db_table and column_name=:column_name"
                        ),
                        table_name=self.db_table,
                        column_name=column,
                    ).fetchall()

                    if len(rows) == 0:
                        logging.debug(
                            f"Unable to find column during insert: {column}; Updating table..."
                        )
                        self.conn.execute(
                            text(
                                "ALTER TABLE :db_table ADD COLUMN :column_name :column_type"
                            ),
                            table_name=self.db_table,
                            column_name=column,
                            column_type=DataHelpers.dataframe_sql_columns(
                                df, column_name=column
                            ),
                        )

                # retry insert with missing columns added
                df.to_sql(
                    self.db_table,
                    if_exists=common.DBInsertTypes.Append.value,
                    index=False,
                    method=None,
                    con=self.conn,
                )
        elif self.format in [DataHandlerTypes.SQLITE, DataHandlerCliTypes.SQLITE]:
            # sync each row of the report to the database
            df = pd.DataFrame([row])
            # determine special column handling for json data
            if not self.dtypes:
                dtypes = {}
                for k in row.keys():
                    if isinstance(row[k], dict) or isinstance(row[k], list):
                        dtypes[k] = sqlalchemy.types.JSON
            else:
                dtypes = self.dtypes

            try:
                df.to_sql(
                    name=self.db_table,
                    con=self.conn,
                    index=False,
                    if_exists=common.DBInsertTypes.Append.value,
                    dtype=dtypes,
                )
            # handle cases where json data has inconsistent rows (add missing here)
            except sqlalchemy.exc.OperationalError as e:
                if re.search(r" table \S+ has no column named", str(e)):
                    ddl = "SELECT * FROM {table_name} LIMIT 1"
                    sql_command = ddl.format(table_name=self.db_table)
                    result = self.conn.execute(text(sql_command)).fetchall()[0].keys()
                    columns = [x for x in result]
                    missing_columns = [x for x in row.keys() if str(x) not in columns]
                    for column in missing_columns:
                        logging.debug(
                            f"Unable to find column during insert: {column}; Updating table..."
                        )

                        # determine the column type
                        if isinstance(row[column], list) or isinstance(
                            row[column], dict
                        ):
                            column_type = "JSON"
                        elif isinstance(row[column], int):
                            column_type = "INTEGER"
                        else:
                            column_type = "TEXT"

                        ddl = "ALTER TABLE {table_name} ADD column {column_name} {column_type}"
                        sql_command = text(
                            ddl.format(
                                table_name=self.db_table,
                                column_name=column,
                                column_type=column_type,
                            )
                        )
                        self.conn.execute(sql_command)

                    # retry adding row
                    df.to_sql(
                        name=self.db_table,
                        con=self.conn,
                        index=False,
                        if_exists=common.DBInsertTypes.Append.value,
                        dtype=dtypes,
                    )
            except Exception as e:
                logging.critical(e)
        else:
            logging.error(f"Unkown format type: {self.format}")

    def show_sample(self, row):
        logging.warn("Sampling only no action will be taken")
        print(json.dumps(row, indent=4))

    def get(self):
        return self.dataset

    def __enter__(self):
        if not self.sample:
            self.__open()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not self.sample:
            self.__close()


class QueryHandler:
    def __init__(
        self,
        client=None,
        type=None,
        object=None,
        start_time=None,
        end_time=None,
        filters=None,
        returns=None,
        lql_query=None,
        dataset=None,
    ):
        # attempt to get context from config
        if client is None:
            client = common.config.client

        if type is None:
            type = common.config.TYPE

        if object is None:
            object = common.config.OBJECT

        if start_time is None:
            start_time = common.config.start_time

        if end_time is None:
            end_time = common.config.end_time

        if filters is None:
            filters = common.config.filters

        if returns is None:
            returns = common.config.returns

        if lql_query is None:
            lql_query = common.config.lql_query

        if dataset is None:
            dataset = common.config.dataset

        # context if not passed or in config
        if start_time is None:
            start_time = datetime.utcnow() + timedelta(days=-1)

        if end_time is None:
            end_time = datetime.utcnow()

        if filters is None:
            filters = []

        self.client = client
        self.type = type
        self.object = object
        self.start_time = start_time
        self.end_time = end_time
        self.filters = filters
        self.returns = returns
        self.lql_query = lql_query
        self.dataset = dataset

    def execute(self):
        # build query string
        q = {
            "timeFilter": {
                "startTime": self.start_time.strftime(ISO_FORMAT),
                "endTime": self.end_time.strftime(ISO_FORMAT),
            },
            "filters": self.filters,
            "returns": self.returns,
        }

        # handle compliance_evalutations dataset field
        if self.dataset is not None:
            q["dataset"] = self.dataset.value

        # create reference to search object
        if common.ObjectTypes.has_value(self.object) and common.ActionTypes.has_value(
            self.type
        ):
            # special case handing where there is only type and no object
            self.type = self.object
            obj = getattr(self.client, f"{self.type}")
        else:
            obj = getattr(getattr(self.client, f"{self.type}"), f"{self.object}")

        # handle lql style queries - limited to LQL_PAGINATION_MAX results
        if common.ObjectTypes.has_value(self.type) and common.QueriesTypes.has_value(
            self.object
        ):
            response = obj(
                evaluator_id="<<IMPLICIT>>",
                query_text=self.lql_query,
                arguments={
                    "StartTimeRange": self.start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "EndTimeRange": self.end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                },
            )

            result = response.get("data", [])
            logging.debug(f"Query result: {json.dumps(result, indent=4)}")
            num_returned = len(result)
            if num_returned == LQL_PAGINATION_MAX:
                logging.warning(
                    f"Warning! The maximum number of active containers ({LQL_PAGINATION_MAX}) was returned."
                )

            return [response]
        elif common.ObjectTypes.has_value(self.type):
            # return query result reference

            # support legacy API functions migrated to v2
            if common.LegacyV2ObjectTypes.has_value(self.type):
                h = APIv2Helper(self.client._session, obj._object_type)
                return h.search(json=q)
            else:
                return obj.search(json=q)
        else:
            logging.error(
                f"Query type {self.type}.{self.object} currently not supported"
            )
            raise Exception(
                f"Query type {self.type}.{self.object} currently not supported"
            )


class ExportHandler:
    def __init__(
        self,
        format,
        results,
        field_map=None,
        dtypes=None,
        file_path="export.csv",
        template_path=None,
        append=False,
        db_connection=None,
        db_table="export",
        db_if_exists="replace",
        db_create_if_missing=True,
        flatten_json=False,
        sample=False,
    ):
        self.format = format
        self.results = results
        self.field_map = field_map
        self.dtypes = dtypes
        self.file_path = file_path
        self.template_path = template_path
        self.append = append
        self.db_connection = db_connection
        self.db_table = db_table
        self.db_if_exists = db_if_exists
        self.db_create_if_missing = db_create_if_missing
        self.flatten_json = flatten_json
        self.sample = sample

    def export(self):
        with DataHandler(
            format=self.format,
            file_path=self.file_path,
            template_path=self.template_path,
            append=self.append,
            db_connection=self.db_connection,
            db_table=self.db_table,
            db_if_exists=self.db_if_exists,
            db_create_if_missing=self.db_create_if_missing,
            flatten_json=self.flatten_json,
            sample=self.sample,
            dtypes=self.dtypes,
        ) as h:
            # process results
            for result in self.results:
                if len(result["data"]) == 0:
                    logging.warn("Query returned 0 results")
                    # raise Exception("Query returned 0 results")
                else:
                    rows = 0
                    if result["data"][0].get("report") is not None:
                        rows = len(result["data"][0].get("report"))
                    else:
                        rows = len(result["data"])

                    logging.info(f"Processing {rows} rows...")
                    for data in result["data"]:
                        # create the data row
                        try:
                            row = DataHelpers.map_fields(
                                data=data, field_map=self.field_map
                            )
                        except Exception as e:
                            logging.error(f"Failed to map fields for data: {data}")
                            raise Exception(e)

                        # sample data and exit
                        if self.sample:
                            h.show_sample(row)
                            exit(1)
                        # thread exporting results
                        else:
                            with ThreadPoolExecutor(max_workers=5) as exe:
                                futures = []
                                future = exe.submit(h.insert, row)
                                futures.append(future)

            # return
            return h.get()
