from typing import Any, AnyStr

import logging
import re
import tempfile
from enum import Enum
from pathlib import Path

import pandas as pd
import sqlalchemy
from laceworksdk import LaceworkClient
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy_utils.functions import create_database, database_exists


class ComplianceReportCSP(Enum):
    AWS = "AwsCfg"
    GCP = "GcpCfg"
    AZURE = "AzureCfg"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class ComplianceReportTypes(Enum):
    AWS = "AwsCfg"
    GCP = "GcpCfg"
    AZURE = "AzureCfg"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


def get_cloud_accounts(client: LaceworkClient = None) -> list[Any]:
    cloud_accounts = client.cloud_accounts.search(json={})

    accounts: list[Any] = []
    for row in cloud_accounts["data"]:
        if row["type"] == "GcpCfg":
            projectIds = [x for x in row["state"]["details"]["projectErrors"].keys()]
            orgId = (
                row["data"]["id"] if row["data"]["idType"] == "ORGANIZATION" else None
            )
            exists = [
                x
                for x in accounts
                if x["orgId"] == orgId and x["projectIds"] == projectIds
            ]
            if len(exists) == 0:
                data = {
                    "name": row["name"],
                    "isOrg": row["isOrg"],
                    "enabled": row["enabled"],
                    "state": row["state"]["ok"],
                    "type": row["type"],
                    "orgId": orgId,
                    "projectIds": projectIds,
                    "account": None,
                    "tenantId": None,
                    "subscriptionIds": None,
                }
                accounts.append(data)
        elif row["type"] == "AwsCfg":
            account = row["data"]["crossAccountCredentials"]["roleArn"].split(":")[4]
            exists = [x for x in accounts if x["account"] == account]
            if len(exists) == 0:
                data = {
                    "name": row["name"],
                    "isOrg": row["isOrg"],
                    "enabled": row["enabled"],
                    "state": row["state"]["ok"],
                    "type": row["type"],
                    "orgId": None,
                    "projectIds": None,
                    "account": account,
                    "tenantId": None,
                    "subscriptionIds": None,
                }
                accounts.append(data)
        elif row["type"] == "AzureCfg":
            subscriptionIds = [
                x for x in row["state"]["details"]["subscriptionErrors"].keys()
            ]
            tennantId = row["data"]["tenantId"]

            exists = [
                x
                for x in accounts
                if x["tenantId"] == tennantId
                and x["subscriptionIds"] == subscriptionIds
            ]
            if len(exists) == 0:
                data = {
                    "name": row["name"],
                    "isOrg": row["isOrg"],
                    "enabled": row["enabled"],
                    "state": row["state"]["ok"],
                    "type": row["type"],
                    "orgId": None,
                    "projectIds": None,
                    "account": None,
                    "tenantId": tennantId,
                    "subscriptionIds": subscriptionIds,
                }
            accounts.append(data)

    return accounts


def sqlite_sync_report(
    report: Any,
    table_name: AnyStr,
    queries: dict[Any, Any] = {},
    db_path_override: Any = None,
) -> dict[Any, Any]:
    logging.info("Syncing data to cache for stats generation...")
    with tempfile.TemporaryDirectory() as tmpdirname:
        db_table = table_name
        df = pd.DataFrame(report)

        # allow override of db path
        if db_path_override is not None:
            db_path = Path("database.db")
        else:
            db_path = Path(tmpdirname).joinpath("database.db")

        logging.info(f"Creating db: { db_path.absolute() }")

        # connect to the db
        logging.info(f"Connecting: sqlite:///{db_path.absolute()}")
        engine = create_engine(f"sqlite:///{db_path.absolute()}", echo=False)

        # if db doesn't exist create it
        if not database_exists(engine.url):
            create_database(engine.url)

        # connect to the database
        con = engine.connect()

        # drop table if it exists
        metadata = MetaData(bind=con)
        t = Table(db_table, metadata)
        t.drop(con, checkfirst=True)

        # sync each row of the report to the database
        for row in report:
            df = pd.DataFrame([row])
            dtypes = {}
            for k in row.keys():
                if isinstance(row[k], dict) or isinstance(row[k], list):
                    dtypes[k] = sqlalchemy.types.JSON
            try:
                df.to_sql(
                    name=db_table,
                    con=con,
                    index=False,
                    if_exists="append",
                    dtype=dtypes,
                )
            # handle cases where json data has inconsistent rows (add missing here)
            except sqlalchemy.exc.OperationalError as e:
                if re.search(r" table \S+ has no column named", str(e)):
                    ddl = "SELECT * FROM {table_name} LIMIT 1"
                    sql_command = ddl.format(table_name=db_table)
                    result = con.execute(text(sql_command)).fetchall()[0].keys()
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
                                table_name=db_table,
                                column_name=column,
                                column_type=column_type,
                            )
                        )
                        con.execute(sql_command)

                    # retry adding row
                    df.to_sql(
                        name=db_table,
                        con=con,
                        index=False,
                        if_exists="append",
                        dtype=dtypes,
                    )

        logging.info("Data sync complete")

        logging.info("Generating query results")
        results = {}
        for query in queries.keys():
            logging.info(f"Executing query: {query}")
            df = pd.read_sql_query(
                sql=queries[query].replace(":table_name", table_name),
                con=con,
            )
            results[query] = df.to_dict(orient="records")

        logging.info("Queries complete")
        return results
