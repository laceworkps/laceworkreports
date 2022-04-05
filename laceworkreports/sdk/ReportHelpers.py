import typing
from typing import Any
from typing import Dict as typing_dict
from typing import List as typing_list

import logging
import re
import tempfile
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path

import laceworksdk
import pandas as pd
import sqlalchemy
from laceworksdk import LaceworkClient
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy_utils.functions import create_database, database_exists

from laceworkreports import common
from laceworkreports.sdk.DataHandlers import (
    DataHandlerTypes,
    ExportHandler,
    QueryHandler,
)


class ComplianceReportCSP(Enum):
    AWS = "AwsCfg"
    GCP = "GcpCfg"
    AZURE = "AzureCfg"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class AWSComplianceTypes(Enum):
    AWS_CIS_S3 = "AWS_CIS_S3"
    NIST_800_53_Rev4 = "NIST_800-53_Rev4"
    NIST_800_171_Rev2 = "NIST_800-171_Rev2"
    ISO_2700 = "ISO_2700"
    HIPAA = "HIPAA"
    SOC = "SOC"
    AWS_SOC_Rev2 = "AWS_SOC_Rev2"
    PCI = "PCI"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class GCPComplianceTypes(Enum):
    GCP_CIS = "GCP_CIS"
    GCP_SOC = "GCP_SOC"
    GCP_CIS12 = "GCP_CIS12"
    GCP_K8S = "GCP_K8S"
    GCP_PCI_Rev2 = "GCP_PCI_Rev2"
    GCP_SOC_Rev2 = "GCP_SOC_Rev2"
    GCP_HIPAA_Rev2 = "GCP_HIPAA_Rev2"
    GCP_ISO_27001 = "GCP_ISO_27001"
    GCP_NIST_CSF = "GCP_NIST_CSF"
    GCP_NIST_800_53_REV4 = "GCP_NIST_800_53_REV4"
    GCP_NIST_800_171_REV2 = "GCP_NIST_800_171_REV2"
    GCP_PCI = "GCP_PCI"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class AzureComplianceTypes(Enum):
    AZURE_CIS = "AZURE_CIS"
    AZURE_CIS_131 = "AZURE_CIS_131"
    AZURE_SOC = "AZURE_SOC"
    AZURE_SOC_Rev2 = "AZURE_SOC_Rev2"
    AZURE_PCI = "AZURE_PCI"
    AZURE_PCI_Rev2 = "AZURE_PCI_Rev2"
    AZURE_ISO_27001 = "AZURE_ISO_27001"
    AZURE_NIST_CSF = "AZURE_NIST_CSF"
    AZURE_NIST_800_53_REV5 = "AZURE_NIST_800_53_REV5"
    AZURE_NIST_800_171_REV2 = "AZURE_NIST_800_171_REV2"
    AZURE_HIPAA = "AZURE_HIPAA"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class ReportSeverityTypes(Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class ReportHelper:
    def __init__(self) -> None:
        self.reports: typing_list[Any] = []
        self.subaccounts: typing_list[Any] = []

    def report_callback(self, future):
        report = future.result()
        if report is not None:
            self.reports = self.reports + report

    def get_reports(self):
        return self.reports

    def get_subaccounts(self, client: LaceworkClient = None) -> typing_list[Any]:
        org_info = client.organization_info.get()
        is_org = False
        org_admin = False
        subaccounts = []

        for i in org_info["data"]:
            is_org = i["orgAccount"]

        if is_org:
            logging.info("Organization info found")

            profile = client.user_profile.get()
            for p in profile["data"]:
                org_admin = p["orgAdmin"]
                if org_admin:
                    logging.info("Current account is org admin")
                    for subaccount in p["accounts"]:
                        subaccounts.append(subaccount)
                else:
                    logging.warning(
                        "Current account is not org admin - subaccounts enumeration will be skipped"
                    )
        else:
            logging.warn(
                "Organization info not found - subaccounts enumeration will be skipped"
            )

        self.subaccounts = subaccounts
        return self.subaccounts

    # get cloud accounts from integrations list
    def get_cloud_accounts(
        self,
        client: LaceworkClient,
        lwAccount: Any,
        start_time: datetime = (datetime.utcnow() - timedelta(hours=25)),
        end_time: datetime = (datetime.utcnow()),
        organization: typing.Any = None,
    ) -> typing_list[Any]:

        cloud_accounts = client.cloud_accounts.search(json={})

        accounts: typing_list[Any] = []
        aws_accounts: typing_list[Any] = []
        gcp_accounts: typing_list[Any] = []
        azure_accounts: typing_list[Any] = []

        for row in cloud_accounts["data"]:
            if row["type"] == "GcpCfg":
                projectIds = [
                    x for x in row["state"]["details"]["projectErrors"].keys()
                ]
                orgId = (
                    row["data"]["id"]
                    if row["data"]["idType"] == "ORGANIZATION"
                    else None
                )

                # allow override of organization - hack for non org integrated accounts
                if (
                    organization is not None and orgId is not None
                ) or orgId is not None:
                    orgId = orgId
                elif organization is not None:
                    orgId = organization

                for projectId in projectIds:
                    accountId = f"gcp:{orgId}:{projectId}"
                    data = {
                        "lwAccount": lwAccount,
                        "accountId": accountId,
                        "name": row["name"],
                        "isOrg": row["isOrg"],
                        "enabled": row["enabled"],
                        "state": row["state"]["ok"],
                        "type": row["type"],
                    }
                    if projectId not in gcp_accounts:
                        accounts.append(data)
                        gcp_accounts.append(projectId)
            elif row["type"] == "AwsCfg":
                account = row["data"]["crossAccountCredentials"]["roleArn"].split(":")[
                    4
                ]
                accountId = f"aws:{account}"
                data = {
                    "lwAccount": lwAccount,
                    "accountId": accountId,
                    "name": row["name"],
                    "isOrg": row["isOrg"],
                    "enabled": row["enabled"],
                    "state": row["state"]["ok"],
                    "type": row["type"],
                }
                if account not in aws_accounts:
                    accounts.append(data)
                    aws_accounts.append(account)
            elif row["type"] == "AzureCfg":
                subscriptionIds = [
                    x for x in row["state"]["details"]["subscriptionErrors"].keys()
                ]
                tenantId = row["data"]["tenantId"]

                for subscriptionId in subscriptionIds:
                    accountId = f"az:{tenantId}:{subscriptionId}"
                    data = {
                        "lwAccount": lwAccount,
                        "accountId": accountId,
                        "name": row["name"],
                        "isOrg": row["isOrg"],
                        "enabled": row["enabled"],
                        "state": row["state"]["ok"],
                        "type": row["type"],
                    }
                    if subscriptionId not in azure_accounts:
                        accounts.append(data)
                        azure_accounts.append(subscriptionId)

        lql_query = f"""
                    Custom_HE_Machine_1 {{
                        source {{
                            LW_HE_MACHINES m
                        }}
                        return distinct {{
                            '{lwAccount}' AS lwAccount,
                            m.TAGS:InstanceId::String AS instanceId,
                            m.TAGS:Account::String AS accountId,
                            m.TAGS:ProjectId::String AS projectId,
                            m.TAGS:VmProvider::String AS VmProvider
                        }}
                    }}
                    """

        machine_accounts = ExportHandler(
            format=DataHandlerTypes.DICT,
            results=QueryHandler(
                client=client,
                start_time=start_time,
                end_time=end_time,
                type=common.ObjectTypes.Queries.value,
                object=common.QueriesTypes.Execute.value,
                lql_query=lql_query,
            ).execute(),
        ).export()

        for m in machine_accounts:
            if (
                m["ACCOUNTID"] is not None
                and m["VMPROVIDER"] == "AWS"
                and m["ACCOUNTID"] not in aws_accounts
            ):
                accountId = f"aws:{m['ACCOUNTID']}"
                data = {
                    "lwAccount": lwAccount,
                    "accountId": accountId,
                    "name": "LQL Discovered",
                    "isOrg": None,
                    "enabled": True,
                    "state": None,
                    "type": "AwsLql",
                }
                if accountId.split(":")[-1] not in aws_accounts:
                    accounts.append(data)
                    aws_accounts.append(m["ACCOUNTID"])

            elif (
                m["PROJECTID"] is not None
                and m["VMPROVIDER"] == "GCE"
                and m["PROJECTID"] not in gcp_accounts
            ):
                accountId = f"gcp::{m['PROJECTID']}"
                data = {
                    "lwAccount": lwAccount,
                    "accountId": accountId,
                    "name": "LQL Discovered",
                    "isOrg": None,
                    "enabled": True,
                    "state": None,
                    "type": "GcpLql",
                }
                if accountId.split(":")[-1] not in gcp_accounts:
                    accounts.append(data)
                    gcp_accounts.append(m["PROJECTID"])
            elif (
                m["PROJECTID"] is not None
                and m["VMPROVIDER"] == "Azure"
                and m["PROJECTID"] not in azure_accounts
            ):
                accountId = f"az::{m['PROJECTID']}"
                data = {
                    "lwAccount": lwAccount,
                    "accountId": accountId,
                    "name": "LQL Discovered",
                    "isOrg": None,
                    "enabled": True,
                    "state": None,
                    "type": "AzureLql",
                }
                if accountId.split(":")[-1] not in azure_accounts:
                    accounts.append(data)
                    azure_accounts.append(m["PROJECTID"])

        return accounts

    def sqlite_sync_report(
        self,
        report: typing.Any,
        table_name: typing.AnyStr,
        queries: typing_dict[typing.Any, typing.Any] = {},
        db_path_override: typing.Any = None,
    ) -> typing_dict[typing.Any, typing.Any]:
        logging.info("Syncing data to cache for stats generation...")
        with tempfile.TemporaryDirectory() as tmpdirname:
            db_table = table_name
            df = pd.DataFrame(report)

            # allow override of db path
            if db_path_override is not None:
                db_path = Path(db_path_override)
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
                        missing_columns = [
                            x for x in row.keys() if str(x) not in columns
                        ]
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
                    sql=queries[query].replace(":db_table", table_name),
                    con=con,
                )
                results[query] = df.to_dict(orient="records")

            logging.info("Queries complete")
            return results

    def sqlite_queries(
        self,
        queries: typing_dict[typing.Any, typing.Any],
        db_table: typing.Any,
        db_connection: typing.Any,
    ) -> typing_dict[typing.Any, typing.Any]:

        logging.info("Generating query results")
        engine = create_engine(db_connection)
        conn = engine.connect()

        results = {}
        for query in queries.keys():
            logging.info(f"Executing query: {query}")
            df = pd.read_sql_query(
                sql=queries[query].replace(":db_table", db_table),
                con=conn,
            )
            results[query] = df.to_dict(orient="records")

        logging.info("Queries complete")
        return results

    def sqlite_drop_table(
        self, db_table: typing.Any, db_connection: typing.Any
    ) -> bool:
        logging.info(f"Attempting to drop table {db_table}...")
        engine = create_engine(db_connection)
        conn = engine.connect()

        if engine.has_table(db_table):
            logging.info(f"Dropping existing table {db_table}")
            metadata = MetaData(bind=conn)
            t = Table(db_table, metadata)
            t.drop(conn, checkfirst=True)
        else:
            logging.info(f"Table {db_table} does not exist")

        return True

    def get_compliance_report(
        self,
        client: LaceworkClient,
        lwAccount: typing.Any,
        cloud_account: typing.Any,
        # start_time: datetime = (datetime.utcnow() - timedelta(hours=25)),
        # end_time: datetime = (datetime.utcnow()),
        aws_compliance: AWSComplianceTypes = AWSComplianceTypes.AWS_CIS_S3,
        gcp_compliance: GCPComplianceTypes = GCPComplianceTypes.GCP_CIS12,
        azure_compliance: AzureComplianceTypes = AzureComplianceTypes.AZURE_CIS_131,
        ignore_errors: bool = True,
        organization: typing.Any = None,
    ) -> typing.Any:
        result = []
        cloud_account_details = cloud_account.split(":")
        csp = cloud_account_details[0]
        if csp == "aws":
            accountId = cloud_account_details[1]
            try:
                report = client.compliance.get_latest_aws_report(
                    aws_account_id=accountId,
                    file_format="json",
                    report_type=aws_compliance.value,
                )
                r = report["data"].pop()
                r["accountId"] = cloud_account
                r["lwAccount"] = lwAccount
                result.append(r)
            except laceworksdk.exceptions.ApiError as e:
                logging.error(f"Lacework api returned: {e}")

                if not ignore_errors:
                    raise e
        elif csp == "gcp":
            csp, orgId, projectId = cloud_account_details
            # requires special case handling as there are cases where orgId is not available via API
            if organization is None and orgId is None:
                logging.warn(
                    f"Skipping GCP projectId:{cloud_account['projectIds']}, organization available and not specified (use --organization)"
                )
                if not ignore_errors:
                    raise Exception(
                        f"GCP projectId:{cloud_account['projectIds']} missing organization (use --organization)"
                    )
            else:
                # when org is available use it
                if (
                    organization is not None and orgId is not None
                ) or orgId is not None:
                    orgId = orgId
                elif organization is not None:
                    orgId = organization

                try:
                    report = client.compliance.get_latest_gcp_report(
                        gcp_organization_id=orgId,
                        gcp_project_id=projectId,
                        file_format="json",
                        report_type=gcp_compliance.value,
                    )
                    r = report["data"].pop()
                    r["accountId"] = cloud_account
                    r["lwAccount"] = lwAccount
                    r.pop("organizationId")
                    r.pop("projectId")
                    result.append(r)
                except laceworksdk.exceptions.ApiError as e:
                    logging.error(f"Lacework api returned: {e}")

                    if not ignore_errors:
                        raise e
        elif csp == "az":
            csp, tenantId, subscriptionId = cloud_account_details
            try:
                report = client.compliance.get_latest_azure_report(
                    azure_tenant_id=tenantId,
                    azure_subscription_id=subscriptionId,
                    file_format="json",
                    report_type=azure_compliance.value,
                )
                r = report["data"].pop()
                r["accountId"] = cloud_account
                r["lwAccount"] = lwAccount
                r.pop("tenantId")
                r.pop("subscriptionId")
                result.append(r)
            except laceworksdk.exceptions.ApiError as e:
                logging.error(f"Lacework api returned: {e}")

                if not ignore_errors:
                    raise e

        return result

    # machines with agents
    def get_active_machines(
        self,
        client: LaceworkClient,
        lwAccount: typing.Any,
        cloud_account: typing.Any,
        start_time: datetime = (datetime.utcnow() - timedelta(hours=25)),
        end_time: datetime = (datetime.utcnow()),
        ignore_errors: bool = True,
        use_sqlite: bool = False,
        db_table: typing.Any = None,
        db_connection: typing.Any = None,
    ) -> typing_list[typing.Any]:
        result = []
        if use_sqlite:
            format_type = DataHandlerTypes.SQLITE
        else:
            format_type = DataHandlerTypes.DICT

        cloud_account_details = cloud_account.split(":")
        csp = cloud_account_details[0]

        if csp == "aws":
            csp, accountId = cloud_account_details
            if accountId != "*":
                filter = f"m.TAGS:Account::String = '{accountId}' AND m.TAGS:VmProvider::String IN ('AWS')"
            else:
                filter = None
            accountId = "'aws:' || m.TAGS:Account::String AS accountId,"
        elif csp == "gcp":
            csp, organizationId, projectId = cloud_account_details
            if projectId != "*":
                filter = f"m.TAGS:ProjectId::String = '{projectId}' AND m.TAGS:VmProvider::String IN ('GCE')"
            else:
                filter = None
            accountId = f"'gcp:' || '{organizationId}' || ':' ||  m.TAGS:ProjectId::String AS accountId,"
        elif csp == "az":
            csp, tenantId, subscriptionId = cloud_account_details
            if subscriptionId != "*":
                filter = f"m.TAGS:ProjectId::String = '{subscriptionId}' AND m.TAGS:VmProvider::String IN ('Azure')"
            else:
                filter = None
            accountId = f"'az:' || '{tenantId}' || ':' ||  m.TAGS:ProjectId::String AS accountId,"

        lql_query = f"""
                    Custom_HE_Machine_1 {{
                        source {{
                            LW_HE_MACHINES m
                        }}
                        filter {{
                            {filter}
                        }}
                        return distinct {{
                            '{lwAccount}' AS lwAccount,
                            {accountId}
                            m.TAGS:InstanceId::String AS tag_instanceId,
                            m.TAGS:Account::String AS tag_accountId,
                            m.TAGS:ProjectId::String AS tag_projectId,
                            m.TAGS:VmProvider::String AS tag_VmProvider,
                            m.TAGS:LwTokenShort::String AS lwTokenShort
                        }}
                    }}
                    """
        try:
            result = ExportHandler(
                format=format_type,
                results=QueryHandler(
                    client=client,
                    start_time=start_time,
                    end_time=end_time,
                    type=common.ObjectTypes.Queries.value,
                    object=common.QueriesTypes.Execute.value,
                    lql_query=lql_query,
                ).execute(),
                db_connection=db_connection,
                db_table=db_table,
            ).export()

        except laceworksdk.exceptions.ApiError as e:
            logging.error(f"Lacework api returned: {e}")

            if not ignore_errors:
                raise e

        return result

    # cloud accounts with ec2 or gce instances
    def get_discovered_cloud_accounts(
        self,
        client: LaceworkClient,
        lwAccount: typing.Any,
        start_time: datetime = (datetime.utcnow() - timedelta(hours=25)),
        end_time: datetime = (datetime.utcnow()),
        ignore_errors: bool = True,
        use_sqlite: bool = False,
        db_table: typing.Any = None,
        db_connection: typing.Any = None,
    ) -> typing_list[typing.Any]:
        results: typing_list[typing.Any] = []
        if use_sqlite:
            format_type = DataHandlerTypes.SQLITE
        else:
            format_type = DataHandlerTypes.DICT

        # get distinct cloud accounts with ec2 instances
        lql_query = f"""ECS {{
                        source {{LW_CFG_AWS_EC2_INSTANCES m}}
                        return distinct {{ 
                                '{lwAccount}' AS lwAccount,
                                'aws:' || m.ACCOUNT_ID AS accountId
                            }}
                        }}
                        """
        # sync results
        try:
            result = ExportHandler(
                format=format_type,
                results=QueryHandler(
                    client=client,
                    start_time=start_time,
                    end_time=end_time,
                    type=common.ObjectTypes.Queries.value,
                    object=common.QueriesTypes.Execute.value,
                    lql_query=lql_query,
                ).execute(),
                db_connection=db_connection,
                db_table=db_table,
            ).export()
            results = results + result
        except laceworksdk.exceptions.ApiError as e:
            logging.error(f"Lacework api returned: {e}")

            if not ignore_errors:
                raise e

        # get distinct cloud accounts with gce instances
        lql_query = f"""
                        GCE {{
                            source {{
                                LW_CFG_GCP_ALL m
                            }}
                            filter {{
                                m.SERVICE = 'compute'
                                AND m.API_KEY = 'resource'
                                AND KEY_EXISTS(m.RESOURCE_CONFIG:status)
                                AND KEY_EXISTS(m.RESOURCE_CONFIG:machineType)
                            }}
                            return distinct {{ 
                                '{lwAccount}' AS lwAccount,
                                'gcp:' || ORGANIZATION::String || ':' || SUBSTRING(
                                    SUBSTRING(
                                        m.URN,
                                        CHAR_INDEX(
                                            '/', 
                                            m.URN
                                        )+34,
                                        LENGTH(m.URN)
                                    ),
                                    0,
                                    CHAR_INDEX(
                                        '/zones/',
                                        SUBSTRING(
                                            m.URN,
                                            CHAR_INDEX(
                                                '/', 
                                                m.URN
                                            )+35,
                                            LENGTH(m.URN)
                                        )
                                    )
                                ) AS accountId
                            }}
                        }}
                        """
        # sync results
        try:
            result = ExportHandler(
                format=format_type,
                results=QueryHandler(
                    client=client,
                    start_time=start_time,
                    end_time=end_time,
                    type=common.ObjectTypes.Queries.value,
                    object=common.QueriesTypes.Execute.value,
                    lql_query=lql_query,
                ).execute(),
                db_connection=db_connection,
                db_table=db_table,
            ).export()
            results = results + result
        except laceworksdk.exceptions.ApiError as e:
            logging.error(f"Lacework api returned: {e}")

            if not ignore_errors:
                raise e

        return results

    # cloud accounts with agents deployed
    def get_active_cloud_accounts(
        self,
        client: LaceworkClient,
        lwAccount: typing.Any,
        start_time: datetime = (datetime.utcnow() - timedelta(hours=25)),
        end_time: datetime = (datetime.utcnow()),
        ignore_errors: bool = True,
        use_sqlite: bool = False,
        db_table: typing.Any = None,
        db_connection: typing.Any = None,
    ) -> typing_list[typing.Any]:
        results: typing_list[typing.Any] = []
        if use_sqlite:
            format_type = DataHandlerTypes.SQLITE
        else:
            format_type = DataHandlerTypes.DICT

        for csp in ["aws", "gcp", "az"]:
            if csp == "aws":
                filter = f"m.TAGS:VmProvider::String IN ('AWS')"
                accountId = "'aws:' || m.TAGS:Account::String AS accountId"
            elif csp == "gcp":
                filter = "m.TAGS:VmProvider::String IN ('GCE')"
                accountId = f"'gcp:' ||  m.TAGS:ProjectId::String AS accountId"
            elif csp == "az":
                filter = "m.TAGS:VmProvider::String IN ('Azure')"
                accountId = f"'az:' ||  m.TAGS:ProjectId::String AS accountId"

            lql_query = f"""
                        Custom_HE_Machine_1 {{
                            source {{
                                LW_HE_MACHINES m
                            }}
                            filter {{
                                {filter}
                            }}
                            return distinct {{
                                '{lwAccount}' AS lwAccount,
                                {accountId}
                            }}
                        }}
                        """
            try:
                result = ExportHandler(
                    format=format_type,
                    results=QueryHandler(
                        client=client,
                        start_time=start_time,
                        end_time=end_time,
                        type=common.ObjectTypes.Queries.value,
                        object=common.QueriesTypes.Execute.value,
                        lql_query=lql_query,
                    ).execute(),
                    db_connection=db_connection,
                    db_table=db_table,
                ).export()
                results = results + result

            except laceworksdk.exceptions.ApiError as e:
                logging.error(f"Lacework api returned: {e}")

                if not ignore_errors:
                    raise e

        return results

    def get_discovered_machines(
        self,
        client: LaceworkClient,
        lwAccount: typing.Any,
        cloud_account: typing.Any,
        start_time: datetime = (datetime.utcnow() - timedelta(hours=25)),
        end_time: datetime = (datetime.utcnow()),
        ignore_errors: bool = True,
        use_sqlite: bool = False,
        db_table: typing.Any = None,
        db_connection: typing.Any = None,
    ) -> typing_list[typing.Any]:
        result: typing_list[typing.Any] = []
        if use_sqlite:
            format_type = DataHandlerTypes.SQLITE
        else:
            format_type = DataHandlerTypes.DICT

        cloud_account_details = cloud_account.split(":")
        csp = cloud_account_details[0]
        lql_query = ""

        if csp == "aws":
            csp, accountId = cloud_account_details

            # skip account filter with wildcard
            if accountId == "*":
                filter = None
            else:
                filter = f"ACCOUNT_ID = '{accountId}'"

            lql_query = f"""ECS {{
                            source {{LW_CFG_AWS_EC2_INSTANCES m}}
                            filter {{ {filter} }}
                            return distinct {{ 
                                    '{lwAccount}' AS lwAccount,
                                    'aws:' || m.ACCOUNT_ID AS accountId, 
                                    m.RESOURCE_ID AS instanceId,
                                    m.RESOURCE_CONFIG:State.Name::String AS state
                                }}
                            }}
                            """
        elif csp == "gcp":
            csp, organizationId, projectId = cloud_account_details

            # skip organization and project filter with wildcard
            if organizationId == "*" and projectId == "*":
                filter = None
            # filter both organization and project
            elif organizationId != "*" and projectId != "*":
                filter = f"""
                                AND ORGANIZATION = {organizationId}
                                AND CONTAINS(m.URN, '://compute.googleapis.com/projects/{projectId}/')
                            """
            # filter only organization
            elif organizationId != "*" and projectId == "*":
                filter = f"""
                                AND ORGANIZATION = {organizationId}
                            """
            # filter only project
            elif organizationId == "*" and projectId != "*":
                filter = f"""
                                AND CONTAINS(m.URN, '://compute.googleapis.com/projects/{projectId}/')
                            """

            lql_query = f"""
                            GCE {{
                                source {{
                                    LW_CFG_GCP_ALL m
                                }}
                                filter {{
                                    m.SERVICE = 'compute'
                                    AND m.API_KEY = 'resource'
                                    AND KEY_EXISTS(m.RESOURCE_CONFIG:status)
                                    AND KEY_EXISTS(m.RESOURCE_CONFIG:machineType)
                                    {filter}
                                }}
                                return distinct {{ 
                                    '{lwAccount}' AS lwAccount,
                                    'gcp:' || ORGANIZATION::String || ':' || SUBSTRING(
                                        SUBSTRING(
                                            m.URN,
                                            CHAR_INDEX(
                                                '/', 
                                                m.URN
                                            )+34,
                                            LENGTH(m.URN)
                                        ),
                                        0,
                                        CHAR_INDEX(
                                            '/zones/',
                                            SUBSTRING(
                                                m.URN,
                                                CHAR_INDEX(
                                                    '/', 
                                                    m.URN
                                                )+35,
                                                LENGTH(m.URN)
                                            )
                                        )
                                    ) AS accountId,
                                    m.RESOURCE_CONFIG:id::string AS instanceId,
                                    m.RESOURCE_CONFIG:status::String AS state
                                }}
                            }}
                            """
        # elif csp == "az":
        #     csp, tenantId, subscriptionId = cloud_account_details
        #     filter = f"m.TAGS:ProjectId::String = '{subscriptionId}' AND m.TAGS:VmProvider::String IN ('Azure')"

        # pull a list of ec2 instance details for the current account
        else:
            logging.warn(f"Unsupported cloud provider type: {cloud_account}")
            return result

        try:
            result = ExportHandler(
                format=format_type,
                results=QueryHandler(
                    client=client,
                    start_time=start_time,
                    end_time=end_time,
                    type=common.ObjectTypes.Queries.value,
                    object=common.QueriesTypes.Execute.value,
                    lql_query=lql_query,
                ).execute(),
                db_connection=db_connection,
                db_table=db_table,
            ).export()

        except laceworksdk.exceptions.ApiError as e:
            logging.error(f"Lacework api returned: {e}")

            if not ignore_errors:
                raise e

        return result

    def get_vulnerability_report(
        self,
        client: LaceworkClient,
        lwAccount: typing.Any,
        cloud_account: typing.Any,
        ignore_errors: bool = True,
        fixable: bool = True,
        severity: ReportSeverityTypes = ReportSeverityTypes.HIGH,
        namespace: typing.Any = None,
        start_time: typing.Any = None,
        end_time: typing.Any = None,
        cve: typing.Any = None,
        use_sqlite: bool = False,
        db_table: typing.Any = None,
        db_connection: typing.Any = None,
    ) -> typing.Any:
        result = []

        try:
            fixable_val = 0
            if fixable:
                fixable_val = 1

            if severity.value == ReportSeverityTypes.CRITICAL.value:
                severity_types = ["Critical"]
            elif severity.value == ReportSeverityTypes.HIGH.value:
                severity_types = ["Critical", "High"]
            elif severity.value == ReportSeverityTypes.MEDIUM.value:
                severity_types = ["Critical", "High", "Medium"]
            elif severity.value == ReportSeverityTypes.LOW.value:
                severity_types = ["Critical", "High", "Medium", "Low"]
            elif severity.value == ReportSeverityTypes.INFO.value:
                severity_types = ["Critical", "High", "Medium", "Low", "Info"]

            filters = [
                {
                    "field": "status",
                    "expression": "in",
                    "values": ["New", "Active", "Reopened"],
                },
                {
                    "field": "severity",
                    "expression": "in",
                    "values": severity_types,
                },
                {
                    "field": "fixInfo.fix_available",
                    "expression": "eq",
                    "value": fixable_val,
                },
            ]

            if namespace is not None:
                filters.append(
                    {
                        "field": "featureKey.namespace",
                        "expression": "rlike",
                        "value": namespace,
                    }
                )

            if cve is not None:
                filters.append({"field": "vulnId", "expression": "rlike", "value": cve})

            cloud_account_details = cloud_account.split(":")
            csp = cloud_account_details[0]

            if csp == "aws":
                csp, accountId = cloud_account_details
                filters.append(
                    {
                        "field": "machineTags.VmProvider",
                        "expression": "in",
                        "values": ["AWS"],
                    }
                )
                filters.append(
                    {
                        "field": "machineTags.Account",
                        "expression": "eq",
                        "value": accountId,
                    }
                )
            elif csp == "gcp":
                csp, orgId, projectId = cloud_account_details
                filters.append(
                    {
                        "field": "machineTags.VmProvider",
                        "expression": "eq",
                        "value": "GCE",
                    }
                )
                filters.append(
                    {
                        "field": "machineTags.ProjectId",
                        "expression": "eq",
                        "value": projectId,
                    }
                )
            elif csp == "az":
                csp, tenantId, subscriptionId = cloud_account_details
                filters.append(
                    {
                        "field": "machineTags.VmProvider",
                        "expression": "in",
                        "values": ["Azure"],
                    }
                )
                filters.append(
                    {
                        "field": "machineTags.ProjectId",
                        "expression": "in",
                        "values": [subscriptionId],
                    }
                )
            if use_sqlite:
                format_type = DataHandlerTypes.SQLITE
            else:
                format_type = DataHandlerTypes.DICT

            # export results
            report = ExportHandler(
                format=format_type,
                results=QueryHandler(
                    client=client,
                    type=common.ObjectTypes.Vulnerabilities.value,
                    object=common.VulnerabilitiesTypes.Hosts.value,
                    start_time=start_time,
                    end_time=end_time,
                    filters=filters,
                    returns=[
                        "startTime",
                        "endTime",
                        "severity",
                        "status",
                        "vulnId",
                        "mid",
                        "featureKey",
                        "machineTags",
                        "fixInfo",
                        "cveProps",
                    ],
                ).execute(),
                db_connection=db_connection,
                db_table=db_table,
            ).export()

            # add the cloud account and lwaccount context
            if use_sqlite:
                db_engine = create_engine(db_connection)
                if db_engine.has_table(db_table):
                    conn = db_engine.connect()
                    ddl = "SELECT * FROM {table_name} LIMIT 1"
                    sql_command = ddl.format(table_name=db_table)
                    result = conn.execute(text(sql_command)).fetchall()[0].keys()
                    columns = [x for x in result]

                    if "accountId" not in columns or "lwAccount" not in columns:
                        for column in ["accountId", "lwAccount"]:
                            ddl = "ALTER TABLE {table_name} ADD column {column_name} {column_type}"
                            sql_command = text(
                                ddl.format(
                                    table_name=db_table,
                                    column_name=column,
                                    column_type="TEXT",
                                )
                            )
                            conn.execute(sql_command)

                    for column in ["accountId", "lwAccount"]:
                        if column == "accountId":
                            column_value = cloud_account
                        elif column == "lwAccount":
                            column_value = lwAccount

                        ddl = "UPDATE {table_name} SET {column_name} = '{column_value}' WHERE {column_name} IS NULL"
                        sql_command = text(
                            ddl.format(
                                table_name=db_table,
                                column_name=column,
                                column_value=column_value,
                            )
                        )
                        conn.execute(sql_command)
                else:
                    logging.warn("Skipping update table")

            else:
                for r in report:
                    r["accountId"] = cloud_account
                    r["lwAccount"] = lwAccount
                    result.append(r)

        except laceworksdk.exceptions.ApiError as e:
            logging.error(f"Lacework api returned: {e}")

            if not ignore_errors:
                raise e

        return result


AgentQueries = {
    "report": """
                SELECT 
                    LWACCOUNT AS lwAccount,
                    ACCOUNTID AS accountId,
                    INSTANCEID AS InstanceId,
                    LOWER(STATE) AS state,
                    (SELECT COUNT(*) FROM machines AS m WHERE m.TAG_INSTANCEID = dm.INSTANCEID) AS has_agent,
                    (SELECT LWTOKENSHORT FROM machines AS m WHERE m.TAG_INSTANCEID = dm.INSTANCEID) AS lwTokenShort
                FROM 
                    :db_table AS dm
                ORDER BY
                    LWACCOUNT,
                    ACCOUNTID
                """,
    "account_coverage": """
                        SELECT 
                            LWACCOUNT AS lwAccount,
                            ACCOUNTID AS accountId,
                            SUM(HAS_AGENT) AS total_installed,
                            COUNT(*) AS total,
                            SUM(HAS_AGENT)*100/COUNT(*) AS total_coverage
                        FROM 
                            (
                                SELECT 
                                    LWACCOUNT,
                                    ACCOUNTID,
                                    INSTANCEID,
                                    LOWER(STATE) AS STATE,
                                    (SELECT COUNT(*) FROM machines AS m WHERE m.TAG_INSTANCEID = dm.INSTANCEID) AS HAS_AGENT,
                                    (SELECT LWTOKENSHORT FROM machines AS m WHERE m.TAG_INSTANCEID = dm.INSTANCEID) AS LWTOKENSHORT
                                FROM 
                                    :db_table AS dm
                            ) AS t
                        WHERE
                            STATE = 'running'
                        GROUP BY
                            LWACCOUNT,
                            ACCOUNTID
                        ORDER BY
                            LWACCOUNT,
                            ACCOUNTID
                        """,
    "total_summary": """
                        SELECT  
                            'Any' AS lwAccount,
                            COUNT(DISTINCT ACCOUNTID) AS total_accounts,
                            SUM(HAS_AGENT) AS total_installed,
                            COUNT(*)-SUM(HAS_AGENT) AS total_not_installed,
                            COUNT(*) AS total,
                            SUM(HAS_AGENT)*100/COUNT(*) AS total_coverage
                        FROM 
                            (
                                SELECT 
                                    LWACCOUNT,
                                    ACCOUNTID,
                                    INSTANCEID,
                                    LOWER(STATE) AS STATE,
                                    (SELECT COUNT(*) FROM machines AS m WHERE m.TAG_INSTANCEID = dm.INSTANCEID) AS HAS_AGENT,
                                    (SELECT LWTOKENSHORT FROM machines AS m WHERE m.TAG_INSTANCEID = dm.INSTANCEID) AS LWTOKENSHORT
                                FROM 
                                    :db_table AS dm
                            ) AS t 
                        WHERE
                            STATE = 'running'
                        """,
    "lwaccount_summary": """
                        SELECT  
                            LWACCOUNT AS lwAccount,
                            COUNT(DISTINCT ACCOUNTID) AS total_accounts,
                            SUM(HAS_AGENT) AS total_installed,
                            COUNT(*)-SUM(HAS_AGENT) AS total_not_installed,
                            COUNT(*) AS total,
                            SUM(HAS_AGENT)*100/COUNT(*) AS total_coverage
                        FROM 
                            (
                                SELECT 
                                    LWACCOUNT,
                                    ACCOUNTID,
                                    INSTANCEID,
                                    LOWER(STATE) AS STATE,
                                    (SELECT COUNT(*) FROM machines AS m WHERE m.TAG_INSTANCEID = dm.INSTANCEID) AS HAS_AGENT,
                                    (SELECT LWTOKENSHORT FROM machines AS m WHERE m.TAG_INSTANCEID = dm.INSTANCEID) AS LWTOKENSHORT
                                FROM 
                                    :db_table AS dm
                            ) AS t  
                        WHERE
                            STATE = 'running'
                        GROUP BY
                            LWACCOUNT
                        """,
    "lwaccount": """
                    SELECT 
                        DISTINCT 
                        LWACCOUNT AS lwAccount
                    FROM
                        (
                            SELECT 
                                LWACCOUNT,
                                ACCOUNTID,
                                INSTANCEID,
                                LOWER(STATE) AS STATE,
                                (SELECT COUNT(*) FROM machines AS m WHERE m.TAG_INSTANCEID = dm.INSTANCEID) AS HAS_AGENT,
                                (SELECT LWTOKENSHORT FROM machines AS m WHERE m.TAG_INSTANCEID = dm.INSTANCEID) AS LWTOKENSHORT
                            FROM 
                                :db_table AS dm
                        ) AS t 
                    """,
}

ComplianceQueries = {
    "report": """
                select 
                    reportType,
                    reportTime,
                    reportTitle,
                    accountId,
                    lwAccount,
                    json_extract(json_recommendations.value, '$.TITLE') AS title,
                    json_extract(json_recommendations.value, '$.INFO_LINK') AS info_link,
                    json_extract(json_recommendations.value, '$.REC_ID') AS rec_id,
                    json_extract(json_recommendations.value, '$.STATUS') AS status,
                    json_extract(json_recommendations.value, '$.CATEGORY') AS category,
                    json_extract(json_recommendations.value, '$.SERVICE') AS service,
                    json_extract(json_recommendations.value, '$.VIOLATIONS') AS violations,
                    json_extract(json_recommendations.value, '$.SUPPRESSIONS') AS suppressions,
                    json_extract(json_recommendations.value, '$.RESOURCE_COUNT') AS resource_count,
                    json_extract(json_recommendations.value, '$.ASSESSED_RESOURCE_COUNT') AS assessed_resource_count,
                    json_array_length(json_extract(json_recommendations.value, '$.VIOLATIONS')) as violation_count,
                    json_array_length(json_extract(json_recommendations.value, '$.SUPPRESSIONS')) as suppression_count,
                    CASE
                        WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 1 THEN 'info'
                        WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 2 THEN 'low'
                        WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 3 THEN 'medium'
                        WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 4 THEN 'high'
                        WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 5 THEN 'critical'
                    END AS severity,
                    json_extract(json_recommendations.value, '$.SEVERITY') AS severity_number,
                    CASE
                        WHEN json_array_length(json_extract(json_recommendations.value, '$.VIOLATIONS')) > json_extract(json_recommendations.value, '$.ASSESSED_RESOURCE_COUNT') THEN 100
                        ELSE CAST(100-cast(json_array_length(json_extract(json_recommendations.value, '$.VIOLATIONS')) AS FLOAT)*100/json_extract(json_recommendations.value, '$.ASSESSED_RESOURCE_COUNT') AS INTEGER)
                    END AS percent
                from 
                    :db_table, 
                    json_each(:db_table.recommendations) AS json_recommendations
                where
                    percent < 100 AND status != 'Compliant'
                order by
                    accountId,
                    reportType,
                    rec_id
                """,
    "account_coverage": """
                        SELECT 
                            t.accountId,
                            t.lwAccount,
                            CASE
                                WHEN SUM(total_violation_count) > SUM(total_assessed_resource_count) THEN 100
                                ELSE 100-SUM(total_violation_count)*100/SUM(total_assessed_resource_count)
                            END AS total_coverage,
                            CASE 
                                WHEN CAST(SUM(total_assessed_resource_count) AS INTEGER) IS NULL THEN 0 
                                ELSE CAST(SUM(total_assessed_resource_count) AS INTEGER)
                            END AS total_assessed_resource_count,
                            CASE 
                                WHEN CAST(SUM(total_violation_count) AS INTEGER) IS NULL THEN 0 
                                ELSE CAST(SUM(total_violation_count) AS INTEGER)
                            END AS total_violation_count,
                            SUM(
                                CASE
                                    WHEN severity_number = 1 THEN total_violation_count
                                    ELSE 0
                                END
                            ) AS critical,
                            SUM(
                                CASE
                                    WHEN severity_number = 2 THEN total_violation_count
                                    ELSE 0
                                END
                            ) AS high,
                            SUM(
                                CASE
                                    WHEN severity_number = 3 THEN total_violation_count
                                    ELSE 0
                                END
                            ) AS medium,
                            SUM(
                                CASE
                                    WHEN severity_number = 4 THEN total_violation_count
                                    ELSE 0
                                END
                            ) AS low,
                            SUM(
                                CASE
                                    WHEN severity_number = 5 THEN total_violation_count
                                    ELSE 0
                                END
                            ) AS info
                        FROM
                            (SELECT
                                lwAccount,
                                accountId,
                                json_extract(json_recommendations.value, '$.ASSESSED_RESOURCE_COUNT') AS total_assessed_resource_count,
                                json_array_length(json_extract(json_recommendations.value, '$.VIOLATIONS')) as total_violation_count,
                                CASE
                                    WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 1 THEN 'info'
                                    WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 2 THEN 'low'
                                    WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 3 THEN 'medium'
                                    WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 4 THEN 'high'
                                    WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 5 THEN 'critical'
                                END AS severity,
                                json_extract(json_recommendations.value, '$.SEVERITY') AS severity_number
                            FROM
                                :db_table,
                                json_each(:db_table.recommendations) AS json_recommendations
                            ) as t
                        GROUP BY
                            accountId,
                            lwAccount
                        ORDER BY
                            accountId,
                            lwAccount,
                            total_coverage
                        """,
    "total_summary": """
                        SELECT
                            'Any' AS lwAccount,
                            COUNT(DISTINCT accountId) AS total_accounts,
                            CASE
                                WHEN SUM(total_violation_count) > SUM(total_assessed_resource_count) THEN 100
                                ELSE 100-SUM(total_violation_count)*100/SUM(total_assessed_resource_count)
                            END AS total_coverage,
                            CASE 
                                WHEN CAST(SUM(total_assessed_resource_count) AS INTEGER) IS NULL THEN 0 
                                ELSE CAST(SUM(total_assessed_resource_count) AS INTEGER)
                            END AS total_assessed_resource_count,
                            CASE 
                                WHEN CAST(SUM(total_violation_count) AS INTEGER) IS NULL THEN 0 
                                ELSE CAST(SUM(total_violation_count) AS INTEGER)
                            END AS total_violation_count,
                            SUM(
                                CASE
                                    WHEN severity_number = 1 THEN total_violation_count
                                    ELSE 0
                                END
                            ) AS critical,
                            SUM(
                                CASE
                                    WHEN severity_number = 2 THEN total_violation_count
                                    ELSE 0
                                END
                            ) AS high,
                            SUM(
                                CASE
                                    WHEN severity_number = 3 THEN total_violation_count
                                    ELSE 0
                                END
                            ) AS medium,
                            SUM(
                                CASE
                                    WHEN severity_number = 4 THEN total_violation_count
                                    ELSE 0
                                END
                            ) AS low,
                            SUM(
                                CASE
                                    WHEN severity_number = 5 THEN total_violation_count
                                    ELSE 0
                                END
                            ) AS info
                        FROM (
                            SELECT
                                accountId,
                                json_extract(json_recommendations.value, '$.ASSESSED_RESOURCE_COUNT') AS total_assessed_resource_count,
                                json_array_length(json_extract(json_recommendations.value, '$.VIOLATIONS')) as total_violation_count,
                                CASE
                                    WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 1 THEN 'info'
                                    WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 2 THEN 'low'
                                    WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 3 THEN 'medium'
                                    WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 4 THEN 'high'
                                    WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 5 THEN 'critical'
                                END AS severity,
                                json_extract(json_recommendations.value, '$.SEVERITY') AS severity_number
                            FROM
                                :db_table,
                                json_each(:db_table.recommendations) AS json_recommendations
                        ) as t
                        """,
    "lwaccount_summary": """
                            SELECT
                                lwAccount,
                                COUNT(DISTINCT accountId) AS total_accounts,
                                CASE
                                    WHEN SUM(total_violation_count) > SUM(total_assessed_resource_count) THEN 100
                                    ELSE 100-SUM(total_violation_count)*100/SUM(total_assessed_resource_count)
                                END AS total_coverage,
                                CASE 
                                    WHEN CAST(SUM(total_assessed_resource_count) AS INTEGER) IS NULL THEN 0 
                                    ELSE CAST(SUM(total_assessed_resource_count) AS INTEGER)
                                END AS total_assessed_resource_count,
                                CASE 
                                    WHEN CAST(SUM(total_violation_count) AS INTEGER) IS NULL THEN 0 
                                    ELSE CAST(SUM(total_violation_count) AS INTEGER)
                                END AS total_violation_count,
                                SUM(
                                    CASE
                                        WHEN severity_number = 1 THEN total_violation_count
                                        ELSE 0
                                    END
                                ) AS critical,
                                SUM(
                                    CASE
                                        WHEN severity_number = 2 THEN total_violation_count
                                        ELSE 0
                                    END
                                ) AS high,
                                SUM(
                                    CASE
                                        WHEN severity_number = 3 THEN total_violation_count
                                        ELSE 0
                                    END
                                ) AS medium,
                                SUM(
                                    CASE
                                        WHEN severity_number = 4 THEN total_violation_count
                                        ELSE 0
                                    END
                                ) AS low,
                                SUM(
                                    CASE
                                        WHEN severity_number = 5 THEN total_violation_count
                                        ELSE 0
                                    END
                                ) AS info
                            FROM (
                                SELECT
                                    lwAccount,
                                    accountId,
                                    json_extract(json_recommendations.value, '$.ASSESSED_RESOURCE_COUNT') AS total_assessed_resource_count,
                                    json_array_length(json_extract(json_recommendations.value, '$.VIOLATIONS')) as total_violation_count,
                                    CASE
                                        WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 1 THEN 'info'
                                        WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 2 THEN 'low'
                                        WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 3 THEN 'medium'
                                        WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 4 THEN 'high'
                                        WHEN json_extract(json_recommendations.value, '$.SEVERITY') = 5 THEN 'critical'
                                    END AS severity,
                                    json_extract(json_recommendations.value, '$.SEVERITY') AS severity_number
                                FROM
                                    :db_table,
                                    json_each(:db_table.recommendations) AS json_recommendations
                            ) as t
                            GROUP BY
                                lwAccount
                            """,
    "lwaccount": """
                    SELECT 
                        DISTINCT lwaccount
                    FROM
                        :db_table
                    """,
}

VulnerabilityQueries = {
    "report": """
                SELECT
                    "accountId",
                    "lwAccount",
                    "startTime",
                    "endTime",
                    "mid",
                    "vulnId",
                    "status",
                    "severity",
                    json_extract(machineTags, '$.Hostname') AS hostname,
                    json_extract(featureKey, '$.name') AS package_name,
                    json_extract(featureKey, '$.namespace') AS package_namespace,
                    json_extract(featureKey, '$.package_active') AS package_active,
                    json_extract(fixInfo, '$.eval_status') AS package_status,
                    json_extract(featureKey, '$.version_installed') AS version,
                    json_extract(fixInfo, '$.fix_available') AS fix_available,
                    json_extract(fixInfo, '$.fixed_version') AS fixed_version,
                    json_extract(machineTags, '$.Account') AS account,
                    json_extract(machineTags, '$.ProjectId') AS projectId,
                    json_extract(machineTags, '$.InstanceId') AS instanceId,
                    json_extract(machineTags, '$.AmiId') AS amiId,
                    json_extract(machineTags, '$.Env') AS env,
                    json_extract(machineTags, '$.ExternalIp') AS externalIp,
                    json_extract(machineTags, '$.InternalIp') AS internalIp,
                    json_extract(machineTags, '$.LwTokenShort') AS lwTokenShort,
                    json_extract(machineTags, '$.SubnetId') AS subnetId,
                    json_extract(machineTags, '$.VmInstanceType') AS vmInstanceType,
                    json_extract(machineTags, '$.VmProvider') AS vmProvider,
                    json_extract(machineTags, '$.VpcId') AS vpcId,
                    json_extract(machineTags, '$.Zone') AS zone,
                    json_extract(machineTags, '$.arch') AS arch,
                    json_extract(machineTags, '$.os') AS os
                FROM 
                    :db_table
                """,
    "account_coverage": """
                        SELECT 
                            t.accountId,
                            t.lwAccount,
                            (
                                100-CAST(COUNT(DISTINCT json_extract(machineTags, '$.InstanceId'))*100/m.machine_count AS INTEGER)
                            ) AS total_coverage,
                            COUNT(DISTINCT json_extract(machineTags, '$.InstanceId')) AS total_resources_in_violation_count,
                            m.machine_count AS total_assessed_resource_count,
                            COUNT(*) AS total_violation_count,
                            SUM(
                                CASE
                                    WHEN severity = 'Critical' THEN 1
                                    ELSE 0
                                END
                            ) AS critical,
                            SUM(
                                CASE
                                    WHEN severity = 'High' THEN 1
                                    ELSE 0
                                END
                            ) AS high,
                            SUM(
                                CASE
                                    WHEN severity = 'Medium' THEN 1
                                    ELSE 0
                                END
                            ) AS medium,
                            SUM(
                                CASE
                                    WHEN severity = 'Low' THEN 1
                                    ELSE 0
                                END
                            ) AS low,
                            SUM(
                                CASE
                                    WHEN severity = 'Info' THEN 1
                                    ELSE 0
                                END
                            ) AS info
                        FROM
                            :db_table as t
                        JOIN (
                            SELECT 
                                    lwAccount, 
                                    accountId,
                                    count(*) AS machine_count
                            FROM 
                                    machines 
                            GROUP BY
                                    lwAccount, 
                                    accountId
                            ) as m 
                            ON 
                                    t.accountId = m.ACCOUNTID 
                                    AND 
                                    t.lwAccount = m.LWACCOUNT
                        GROUP BY
                            t.accountId,
                            t.lwAccount
                        ORDER BY
                            t.accountId,
                            t.lwAccount,
                            total_coverage
                        """,
    "total_summary": """
                        SELECT
                            'Any' AS lwAccount,
                            COUNT(DISTINCT t.accountId) AS total_accounts,
                            (
                                100-CAST(COUNT(DISTINCT json_extract(machineTags, '$.InstanceId'))*100/(
                                    SELECT 
                                        count(*) AS machine_count
                                    FROM 
                                        machines
                                ) AS INTEGER)
                            ) AS total_coverage,
                            COUNT(DISTINCT json_extract(machineTags, '$.InstanceId')) AS total_resources_in_violation_count,
                            (
                                    SELECT 
                                        count(*) AS machine_count
                                    FROM 
                                        machines
                            ) AS total_assessed_resource_count,
                            COUNT(*) AS total_violation_count,
                            SUM(
                                CASE
                                    WHEN severity = 'Critical' THEN 1
                                    ELSE 0
                                END
                            ) AS critical,
                            SUM(
                                CASE
                                    WHEN severity = 'High' THEN 1
                                    ELSE 0
                                END
                            ) AS high,
                            SUM(
                                CASE
                                    WHEN severity = 'Medium' THEN 1
                                    ELSE 0
                                END
                            ) AS medium,
                            SUM(
                                CASE
                                    WHEN severity = 'Low' THEN 1
                                    ELSE 0
                                END
                            ) AS low,
                            SUM(
                                CASE
                                    WHEN severity = 'Info' THEN 1
                                    ELSE 0
                                END
                            ) AS info
                        FROM 
                            :db_table as t                             
                        """,
    "lwaccount_summary": """
                            SELECT
                                t.lwAccount,
                                COUNT(DISTINCT t.accountId) AS total_accounts,
                                (
                                    100-CAST(COUNT(DISTINCT json_extract(machineTags, '$.InstanceId'))*100/m.machine_count AS INTEGER)
                                ) AS total_coverage,
                                COUNT(DISTINCT json_extract(machineTags, '$.InstanceId')) AS total_resources_in_violation_count,
                                m.machine_count AS total_assessed_resource_count,
                                COUNT(*) AS total_violation_count,
                                SUM(
                                    CASE
                                        WHEN severity = 'Critical' THEN 1
                                        ELSE 0
                                    END
                                ) AS critical,
                                SUM(
                                    CASE
                                        WHEN severity = 'High' THEN 1
                                        ELSE 0
                                    END
                                ) AS high,
                                SUM(
                                    CASE
                                        WHEN severity = 'Medium' THEN 1
                                        ELSE 0
                                    END
                                ) AS medium,
                                SUM(
                                    CASE
                                        WHEN severity = 'Low' THEN 1
                                        ELSE 0
                                    END
                                ) AS low,
                                SUM(
                                    CASE
                                        WHEN severity = 'Info' THEN 1
                                        ELSE 0
                                    END
                                ) AS info
                            FROM 
                                :db_table as t
                            JOIN (
                                SELECT 
                                        lwAccount, 
                                        count(*) AS machine_count
                                FROM 
                                        machines 
                                GROUP BY
                                        lwAccount
                                ) as m 
                                ON 
                                        t.lwAccount = m.LWACCOUNT
                            GROUP BY
                                t.lwAccount
                            """,
    "lwaccount": """
                    SELECT 
                        DISTINCT lwaccount
                    FROM
                        :db_table
                    """,
}
