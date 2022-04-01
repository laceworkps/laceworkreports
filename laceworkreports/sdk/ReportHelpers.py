import typing
from typing import Any
from typing import Dict as typing_dict
from typing import List as typing_list

import logging
import re
import tempfile
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


class ReportHelper:
    def __init__(self) -> None:
        self.reports: typing_list[Any] = []
        self.subaccounts: typing_list[Any] = []
        self.cloud_accounts: typing_list[Any] = []

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

    def get_cloud_accounts(self, client: LaceworkClient = None) -> typing_list[Any]:
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
                    gcp_accounts = gcp_accounts + projectIds
            elif row["type"] == "AwsCfg":
                account = row["data"]["crossAccountCredentials"]["roleArn"].split(":")[
                    4
                ]
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
                    aws_accounts.append(account)
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
                azure_accounts = azure_accounts + subscriptionIds

        self.cloud_accounts = accounts

        lql_query = """
                    Custom_HE_Machine_1 {
                        source {
                            LW_HE_MACHINES m
                        }
                        return distinct {
                            m.TAGS:InstanceId::String AS instanceId,
                            m.TAGS:Account::String AS accountId,
                            m.TAGS:ProjectId::String AS projectId,
                            m.TAGS:VmProvider::String AS VmProvider
                        }
                    }
                    """

        machine_accounts = ExportHandler(
            format=DataHandlerTypes.DICT,
            results=QueryHandler(
                client=client,
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
                data = {
                    "name": "LQL Discovered",
                    "isOrg": None,
                    "enabled": True,
                    "state": None,
                    "type": "AwsLql",
                    "orgId": None,
                    "projectIds": None,
                    "account": m["ACCOUNTID"],
                    "tenantId": None,
                    "subscriptionIds": None,
                }
                self.cloud_accounts.append(data)
                aws_accounts.append(m["ACCOUNTID"])

            elif (
                m["PROJECTID"] is not None
                and m["VMPROVIDER"] == "GCE"
                and m["PROJECTID"] not in gcp_accounts
            ):
                data = {
                    "name": "LQL Discovered",
                    "isOrg": None,
                    "enabled": True,
                    "state": None,
                    "type": "GcpLql",
                    "orgId": None,
                    "projectIds": [m["PROJECTID"]],
                    "account": None,
                    "tenantId": None,
                    "subscriptionIds": None,
                }
                self.cloud_accounts.append(data)
                gcp_accounts.append(m["PROJECTID"])
            elif (
                m["PROJECTID"] is not None
                and m["VMPROVIDER"] == "Azure"
                and m["PROJECTID"] not in azure_accounts
            ):
                data = {
                    "name": "LQL Discovered",
                    "isOrg": None,
                    "enabled": True,
                    "state": None,
                    "type": "AzureLql",
                    "orgId": None,
                    "projectIds": [m["PROJECTID"]],
                    "account": None,
                    "tenantId": None,
                    "subscriptionIds": None,
                }
                self.cloud_accounts.append(data)
                azure_accounts.append(m["PROJECTID"])

        return self.cloud_accounts

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
                    sql=queries[query].replace(":table_name", table_name),
                    con=con,
                )
                results[query] = df.to_dict(orient="records")

            logging.info("Queries complete")
            return results

    def sqlite_queries(
        self, queries: typing_dict[typing.Any, typing.Any], db_table, db_connection
    ) -> typing_dict[typing.Any, typing.Any]:

        logging.info("Generating query results")
        engine = create_engine(db_connection)
        conn = engine.connect()

        results = {}
        for query in queries.keys():
            logging.info(f"Executing query: {query}")
            df = pd.read_sql_query(
                sql=queries[query].replace(":table_name", db_table),
                con=conn,
            )
            results[query] = df.to_dict(orient="records")

        logging.info("Queries complete")
        return results

    def sqlite_drop_table(self, db_table, db_connection):
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

    def cloud_accounts_format(self, cloud_account, organization=None):
        accounts = []
        if (
            cloud_account["type"] in ["AwsCfg", "AwsLql"]
            and cloud_account["enabled"] == 1
        ):
            accounts.append(f"aws:{cloud_account['account']}")
        elif (
            cloud_account["type"] in ["GcpCfg", "GcpLql"]
            and cloud_account["enabled"] == 1
        ):
            # requires special case handling as there are cases where orgId is not available via API
            orgId = None

            # when org is available use it
            if (
                organization is not None and cloud_account["orgId"] is not None
            ) or cloud_account["orgId"] is not None:
                orgId = cloud_account["orgId"]
            elif organization is not None:
                orgId = organization

            for projectId in cloud_account["projectIds"]:
                accounts.append(f"gcp:{orgId}:{projectId}")
        elif (
            cloud_account["type"] in ["AzureCfg", "AzureLql"]
            and cloud_account["enabled"] == 1
        ):
            for subscriptionId in cloud_account["subscriptionIds"]:
                accounts.append(f"az:{cloud_account['tenantId']}:{subscriptionId}")

        return accounts

    def get_compliance_report(
        self,
        client: LaceworkClient,
        cloud_account: Any,
        account: Any,
        ignore_errors: bool,
        organization: Any = None,
    ) -> Any:
        result = []
        cloud_account_details = cloud_account.split(":")
        csp = cloud_account_details[0]
        if csp == "aws":
            accountId = cloud_account_details[1]
            try:
                report = client.compliance.get_latest_aws_report(
                    aws_account_id=accountId,
                    file_format="json",
                    report_type=None,
                )
                r = report["data"].pop()
                r["accountId"] = cloud_account
                r["lwAccount"] = account["accountName"]
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
                        report_type=None,
                    )
                    r = report["data"].pop()
                    r["accountId"] = cloud_account
                    r["lwAccount"] = account["accountName"]
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
                    report_type=None,
                )
                r = report["data"].pop()
                r["accountId"] = cloud_account
                r["lwAccount"] = account["accountName"]
                r.pop("tenantId")
                r.pop("subscriptionId")
                result.append(r)
            except laceworksdk.exceptions.ApiError as e:
                logging.error(f"Lacework api returned: {e}")

                if not ignore_errors:
                    raise e

        return result

    def get_active_images(
        self,
        client: LaceworkClient,
        account: Any,
        cloud_account: Any,
        ignore_errors=True,
        use_sqlite=False,
        db_table=None,
        db_connection=None,
    ):
        report = []

        # pull all machines using lql use as filter for MID in?

        if use_sqlite:
            format_type = DataHandlerTypes.SQLITE
        else:
            format_type = DataHandlerTypes.DICT

        lql_query = """
                    Custom_HE_Machine_1 {
                        source {
                            LW_HE_MACHINES m
                        }
                        return distinct {
                            m.TAGS:InstanceId::String AS instanceId,
                            m.TAGS:Account::String AS accountId,
                            m.TAGS:ProjectId::String AS projectId,
                            m.TAGS:VmProvider::String AS VmProvider
                        }
                    }
                    """

        machine_accounts = ExportHandler(
            format=DataHandlerTypes.DICT,
            results=QueryHandler(
                client=client,
                type=common.ObjectTypes.Queries.value,
                object=common.QueriesTypes.Execute.value,
                lql_query=lql_query,
            ).execute(),
        ).export()

        try:
            report = ExportHandler(
                format=format_type,
                results=QueryHandler(
                    client=client,
                    type=common.ObjectTypes.Queries.value,
                    object=common.QueriesTypes.Execute.value,
                    lql_query=lql_query,
                ).execute(),
                field_map={"imageId": "IMAGE_ID", "mid": "MID"},
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
                            column_value = account["accountName"]

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
                    r["lwAccount"] = account["accountName"]
                    result.append(r)

        except laceworksdk.exceptions.ApiError as e:
            logging.error(f"Lacework api returned: {e}")

            if not ignore_errors:
                raise e

        return result

    def get_active_machines(
        self,
        client: LaceworkClient,
        account: Any,
        cloud_account: Any,
        ignore_errors=True,
        use_sqlite=False,
        db_table=None,
        db_connection=None,
    ):
        result = []
        if use_sqlite:
            format_type = DataHandlerTypes.SQLITE
        else:
            format_type = DataHandlerTypes.DICT

        cloud_account_details = cloud_account.split(":")
        csp = cloud_account_details[0]

        if csp == "aws":
            csp, accountId = cloud_account_details
            filter = f"m.TAGS:Account::String = '{accountId}' AND m.TAGS:VmProvider::String IN ('AWS')"
        elif csp == "gcp":
            csp, organziationId, projectId = cloud_account_details
            filter = f"m.TAGS:ProjectId::String = '{projectId}' AND m.TAGS:VmProvider::String IN ('GCE')"
        elif csp == "az":
            csp, tenantId, subscriptionId = cloud_account_details
            filter = f"m.TAGS:ProjectId::String = '{subscriptionId}' AND m.TAGS:VmProvider::String IN ('Azure')"

        lql_query = f"""
                    Custom_HE_Machine_1 {{
                        source {{
                            LW_HE_MACHINES m
                        }}
                        filter {{
                            {filter}
                        }}
                        return distinct {{
                            '{account}' AS lwAccount,
                            '{cloud_account}' AS accountId,
                            m.TAGS:InstanceId::String AS tag_instanceId,
                            m.TAGS:Account::String AS tag_accountId,
                            m.TAGS:ProjectId::String AS tag_projectId,
                            m.TAGS:VmProvider::String AS tag_VmProvider
                        }}
                    }}
                    """
        try:
            result = ExportHandler(
                format=format_type,
                results=QueryHandler(
                    client=client,
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
        account: Any,
        cloud_account: Any,
        ignore_errors: bool,
        fixable=True,
        severity=None,
        namespace=None,
        start_time=None,
        end_time=None,
        cve=None,
        use_sqlite=False,
        db_table=None,
        db_connection=None,
    ) -> Any:
        result = []

        try:
            fixable_val = 0
            if fixable:
                fixable_val = 1

            filters = [
                {
                    "field": "status",
                    "expression": "in",
                    "values": ["New", "Active", "Reopened"],
                },
                {
                    "field": "severity",
                    "expression": "in",
                    "values": ["Critical", "High"],
                },
                {
                    "field": "fixInfo.fix_available",
                    "expression": "eq",
                    "value": fixable_val,
                },
            ]

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
                            column_value = account["accountName"]

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
                    r["lwAccount"] = account["accountName"]
                    result.append(r)

        except laceworksdk.exceptions.ApiError as e:
            logging.error(f"Lacework api returned: {e}")

            if not ignore_errors:
                raise e

        return result

    def get_vulnerability_v1_report(
        self,
        client: LaceworkClient,
        cloud_account: Any,
        account: Any,
        ignore_errors: bool,
        fixable=True,
        severity=None,
        namespace=None,
        start_time=None,
        end_time=None,
        cve=None,
    ) -> Any:
        result = []
        try:
            report = client.vulnerabilities.get_host_vulnerabilities(
                fixable=fixable,
                severity=severity,
                namespace=namespace,
                start_time=start_time,
                end_time=end_time,
                cve=cve,
            )
            for r in report["data"]:
                r["accountId"] = cloud_account
                r["lwAccount"] = account["accountName"]
                result.append(r)
        except laceworksdk.exceptions.ApiError as e:
            logging.error(f"Lacework api returned: {e}")

            if not ignore_errors:
                raise e

        return result


AgentQueries = {
    "report": """
                SELECT 
                    * 
                FROM 
                    :table_name
                WHERE
                    State = 'running'
                ORDER BY
                    accountId,
                    lwAccount
                """,
    "account_coverage": """
                        SELECT 
                            lwAccount,
                            accountId,
                            SUM(lacework) AS total_installed,
                            COUNT(*) AS total,
                            SUM(lacework)*100/COUNT(*) AS total_coverage
                        FROM 
                            :table_name 
                        WHERE
                            State = 'running'
                        GROUP BY
                            accountId,
                            lwAccount
                        ORDER BY
                            accountId,
                            total_coverage
                        """,
    "total_summary": """
                        SELECT  
                            'Any' AS lwAccount,
                            COUNT(DISTINCT accountId) AS total_accounts,
                            SUM(lacework) AS total_installed,
                            COUNT(*)-SUM(lacework) AS total_not_installed,
                            COUNT(*) AS total,
                            SUM(lacework)*100/COUNT(*) AS total_coverage
                        FROM 
                            :table_name 
                        WHERE
                            State = 'running'
                        """,
    "lwaccount_summary": """
                        SELECT  
                            lwAccount,
                            COUNT(DISTINCT accountId) AS total_accounts,
                            SUM(lacework) AS total_installed,
                            COUNT(*)-SUM(lacework) AS total_not_installed,
                            COUNT(*) AS total,
                            SUM(lacework)*100/COUNT(*) AS total_coverage
                        FROM 
                            :table_name 
                        WHERE
                            State = 'running'
                        GROUP BY
                            lwAccount
                        """,
    "lwaccount": """
                    SELECT 
                        DISTINCT 
                        lwAccount
                    FROM
                        :table_name
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
                    :table_name, 
                    json_each(:table_name.recommendations) AS json_recommendations
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
                                :table_name,
                                json_each(:table_name.recommendations) AS json_recommendations
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
                                :table_name,
                                json_each(:table_name.recommendations) AS json_recommendations
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
                                    :table_name,
                                    json_each(:table_name.recommendations) AS json_recommendations
                            ) as t
                            GROUP BY
                                lwAccount
                            """,
    "lwaccount": """
                    SELECT 
                        DISTINCT lwaccount
                    FROM
                        :table_name
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
                    :table_name
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
                            :table_name as t
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
                                        lwAccount, 
                                        accountId,
                                        count(*) AS machine_count
                                    FROM 
                                        machines
                                ) AS INTEGER)
                            ) AS total_coverage,
                            COUNT(DISTINCT json_extract(machineTags, '$.InstanceId')) AS total_resources_in_violation_count,
                            (
                                    SELECT 
                                        lwAccount, 
                                        accountId,
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
                            :table_name as t                             
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
                                :table_name as t
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
                        :table_name
                    """,
}

VulnerabilitV1Queries = {
    "report": """
                SELECT 
                    accountId,
                    lwAccount,
                    cve_id,
                    json_extract(json_pacakges.value, '$.name') AS package_name,
                    json_extract(json_pacakges.value, '$.namespace') AS package_namespace,
                    json_extract(json_pacakges.value, '$.version') AS version,
                    json_extract(json_pacakges.value, '$.fixed_version') AS fixed_version,
                    json_extract(json_pacakges.value, '$.fix_available') AS fix_available,
                    json_extract(json_pacakges.value, '$.host_count') AS host_count,
                    json_extract(json_pacakges.value, '$.severity') AS package_severity,
                    (
                        SELECT
                            json_severity.key
                        FROM
                            json_each(json_extract(:table_name.summary, '$.severity')) AS json_severity
                        LIMIT 1
                    ) AS severity,
                    json_extract(json_pacakges.value, '$.cve_link') AS cve_link,
                    json_extract(json_pacakges.value, '$.cvss_score') AS cvss_score,
                    json_extract(json_pacakges.value, '$.cvss_v3_score') AS cvss_v3_score,
                    json_extract(json_pacakges.value, '$.cvss_v2_score') AS cvss_v2_score,
                    json_extract(json_pacakges.value, '$.description') AS description,
                    json_extract(json_pacakges.value, '$.status') AS status,
                    json_extract(json_pacakges.value, '$.package_status') AS package_status,
                    json_extract(json_pacakges.value, '$.last_updated_time') AS last_updated_time,
                    json_extract(json_pacakges.value, '$.first_seen_time') AS first_seen_time,
                    json_extract(:table_name.summary, '$.total_vulnerabilities') AS total_vulnerabilities,
                    json_extract(:table_name.summary, '$.last_evaluation_time') AS last_evaluation_time,
                    json_extract(:table_name.summary, '$.total_exception_vulnerabilities') AS total_exception_vulnerabilities
                FROM 
                    :table_name, 
                    json_each(:table_name.packages) AS json_pacakges
                WHERE
                    state = 'Active'
                ORDER BY
                    accountId,
                    lwAccount
                """
}
