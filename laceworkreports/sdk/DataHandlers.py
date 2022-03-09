"""
Example script showing how to use the LaceworkClient class.
"""

import csv
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path

import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import create_engine
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
    PANDAS = "pandas"
    DICT = "dict"
    CSV = "csv"
    JSON = "json"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class DataHandlerCliTypes(Enum):
    POSTGRES = "postgres"
    CSV = "csv"
    JSON = "json"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class DataHandler:
    def __init__(
        self,
        format,
        file_path="export.csv",
        append=False,
        flatten_json=False,
        dtypes=None,
        db_connection=None,
        db_table="export",
        db_if_exists="replace",
    ):
        logging.info(f"Processing data with format: {format}")
        if not DataHandlerTypes.has_value(format.value):
            raise Exception(
                "Unsupported export format, exepcted {} found: {}".format(
                    list(DataHandlerTypes), format
                )
            )

        self.dataset = []
        self.reader = None
        self.format = format
        self.file_path = file_path
        self.db_connection = db_connection
        self.db_engine = None
        self.db_table = db_table
        self.db_if_exists = db_if_exists
        self.dropped_columns = {}
        self.flatten_json = flatten_json
        self.append = append

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
            self.db_engine = create_engine(self.db_connection)

            # check for the db if it doesn't exist create it
            if not database_exists(self.db_engine.url):
                logging.warn(
                    f"Database not found - creating: {self.db_engine.url.database}"
                )
                create_database(self.db_engine.url)

            # connect to the database
            self.conn = self.db_engine.connect()

            # if we are replacing drop the table first
            if self.db_if_exists == common.DBInsertTypes.Replace:
                self.conn.execute(f"DROP TABLE IF EXISTS {self.db_table}")
            elif (
                self.db_if_exists == common.DBInsertTypes.Fail
                and self.db_engine.has_table(self.db_table)
            ):
                logging.error("Table already exists and db_if_exists=fail")
                raise Exception("Table already exists and db_if_exists=fail")

            # run a test query
            self.conn.execute("SELECT 1 as conn_test")

    def __close(self):
        if self.format in [DataHandlerTypes.CSV, DataHandlerCliTypes.CSV]:
            self.fp.close()

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
        elif self.format == DataHandlerTypes.DICT:
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
                    if_exists=common.DBInsertTypes.Append,
                    index=False,
                    con=self.conn,
                    dtype=dtypes,
                )
            except sqlalchemy.exc.ProgrammingError as e:
                logging.error(e)
                # ensure that any additional columns are added as needed
                for column in df.columns:
                    sql_query = "SELECT column_name FROM information_schema.columns WHERE table_name='{}' and column_name='{}';".format(
                        self.db_table, column
                    )
                    rows = self.conn.execute(sql_query).fetchall()
                    if len(rows) == 0:
                        logging.debug(
                            f"Unable to find column during insert: {column}; Updating table..."
                        )
                        sql_query = 'ALTER TABLE "{}" ADD COLUMN "{}" {};'.format(
                            self.db_table,
                            column,
                            DataHelpers.dataframe_sql_columns(df, column_name=column),
                        )
                        self.conn.execute(sql_query)

                # retry insert with missing columns added
                df.to_sql(
                    self.db_table,
                    if_exists=common.DBInsertTypes.Append,
                    index=False,
                    method=None,
                    con=self.conn,
                )
        else:
            logging.error(f"Unkown format type: {self.format}")

    def get(self):
        return self.dataset

    def __enter__(self):
        self.__open()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
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

        # create reference to search object
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
        append=False,
        db_connection=None,
        db_table="export",
        db_if_exists="replace",
        flatten_json=False,
    ):
        self.format = format
        self.results = results
        self.field_map = field_map
        self.dtypes = dtypes
        self.file_path = file_path
        self.append = append
        self.db_connection = db_connection
        self.db_table = db_table
        self.db_if_exists = db_if_exists
        self.flatten_json = flatten_json

    def export(self):
        with DataHandler(
            format=self.format,
            file_path=self.file_path,
            append=self.append,
            db_connection=self.db_connection,
            db_table=self.db_table,
            db_if_exists=self.db_if_exists,
            flatten_json=self.flatten_json,
            dtypes=self.dtypes,
        ) as h:
            # process results
            for result in self.results:
                if len(result["data"]) == 0:
                    logging.error("Query returned 0 results")
                    # raise Exception("Query returned 0 results")
                else:
                    logging.info(f"Processing {len(result['data'])} rows...")
                    for data in result["data"]:
                        # create the data row
                        try:
                            row = DataHelpers.map_fields(
                                data=data, field_map=self.field_map
                            )
                        except Exception as e:
                            logging.error(f"Failed to map fields for data: {data}")
                            raise Exception(e)

                        with ThreadPoolExecutor(max_workers=5) as exe:
                            futures = []
                            future = exe.submit(h.insert, row)
                            futures.append(future)

            # return
            return h.get()
