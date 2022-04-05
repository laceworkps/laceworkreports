"""
Report Handler
"""

from typing import Optional

import logging
from pathlib import Path

import typer

from laceworkreports import common
from laceworkreports.sdk.DataHandlers import DataHandlerTypes, ExportHandler
from laceworkreports.sdk.ReportHelpers import (
    AWSComplianceTypes,
    AzureComplianceTypes,
    ComplianceQueries,
    GCPComplianceTypes,
    ReportHelper,
)

app = typer.Typer(no_args_is_help=True)


@app.command(no_args_is_help=True, help="Generate HTML report")
def html(
    ctx: typer.Context,
    # current compliance reports do not support time range queries
    # start_time: datetime = typer.Option(
    #     (datetime.utcnow() - timedelta(hours=25)).strftime(common.ISO_FORMAT),
    #     formats=[common.ISO_FORMAT],
    #     help="Start time for query period",
    # ),
    # end_time: datetime = typer.Option(
    #     (datetime.utcnow()).strftime(common.ISO_FORMAT),
    #     formats=[common.ISO_FORMAT],
    #     help="End time for query period",
    # ),
    aws_compliance: AWSComplianceTypes = typer.Option(
        AWSComplianceTypes.AWS_CIS_S3.value,
        help="AWS compliance framework",
    ),
    gcp_compliance: GCPComplianceTypes = typer.Option(
        GCPComplianceTypes.GCP_CIS12.value,
        help="GCP compliance framework",
    ),
    azure_compliance: AzureComplianceTypes = typer.Option(
        AzureComplianceTypes.AZURE_CIS_131.value,
        help="Azure compliance framework",
    ),
    organization: Optional[str] = typer.Option(
        None,
        help="GCP organization id; Required when org level integration is not used",
    ),
    subaccounts: bool = typer.Option(
        False,
        help="Enumerate subaccounts",
        envvar=common.LACEWORK_REPORTS_SUBACCOUNTS,
    ),
    file_path: str = typer.Option(
        ...,
        help="Path to exported result",
        envvar=common.LACEWORK_REPORTS_FILE_PATH,
    ),
    template_path: str = typer.Option(
        Path(__file__).resolve().parent.joinpath("compliance_coverage.html.j2"),
        help="Path to jinja2 template. Results will be passed as 'dataset' variable.",
        envvar=common.LACEWORK_REPORTS_TEMPLATE_PATH,
    ),
    ignore_errors: bool = typer.Option(
        True,
        help="Ignore error for missing reports or inaccessible account details.",
    ),
) -> None:
    """
    Set the command context
    """
    missing_cloud_accounts = []

    # connect lacework client
    lw = common.config.connect()

    # report details
    report_title = "Compliance Coverage"
    db_table = "compliance_coverage"

    reportHelper = ReportHelper()
    db_path = Path("database.db")
    # db_path.unlink(missing_ok=True)
    db_connection = f"sqlite:///{db_path.absolute()}?check_same_thread=False"

    reportHelper.sqlite_drop_table(db_table, db_connection)
    reportHelper.sqlite_drop_table("cloud_accounts", db_connection)

    has_subaccounts = False
    if subaccounts:
        lwAccounts = reportHelper.get_subaccounts(client=lw)
        if len(lwAccounts) == 0:
            logging.error("Subaccounts specificed but none found")
            raise Exception("Subaccounts specificed but none found")
        else:
            has_subaccounts = True
    else:
        lwAccounts = [{"accountName": lw._account}]

    for lwAccount in lwAccounts:
        if has_subaccounts:
            logging.info(f"Switching to subaccount context: {lwAccount['accountName']}")
            lw.set_subaccount(lwAccount["accountName"])

        # get cloud accounts and sync to sqlite
        cloud_accounts = reportHelper.get_cloud_accounts(
            client=lw, lwAccount=lwAccount["accountName"], organization=organization
        )
        ExportHandler(
            format=DataHandlerTypes.SQLITE,
            results=[{"data": cloud_accounts}],
            file_path=file_path,
            db_table="cloud_accounts",
            db_connection=db_connection,
        ).export()

        for cloud_account in cloud_accounts:
            logging.info(
                f"Enumerating {lwAccount['accountName']}:{cloud_account['accountId']}"
            )
            report = reportHelper.get_compliance_report(
                client=lw,
                cloud_account=cloud_account["accountId"],
                lwAccount=lwAccount["accountName"],
                aws_compliance=aws_compliance,
                gcp_compliance=gcp_compliance,
                azure_compliance=azure_compliance,
                ignore_errors=ignore_errors,
                organization=organization,
            )

            if len(report) > 0:
                ExportHandler(
                    format=DataHandlerTypes.SQLITE,
                    results=[{"data": report}],
                    file_path=file_path,
                    db_table=db_table,
                    db_connection=db_connection,
                ).export()
            else:
                missing_cloud_accounts.append(cloud_account["accountId"])

    for miss in missing_cloud_accounts:
        logging.warn(f"missing report for : {miss}")

    # use sqlite query to generate final result
    results = reportHelper.sqlite_queries(
        queries=ComplianceQueries, db_table=db_table, db_connection=db_connection
    )

    if len(results["report"]) > 0:
        report = results["report"]

        # return additional stats under summary
        stats = {}
        for key in [x for x in results.keys() if x != "report"]:
            stats[key] = results[key]

        # write jinja template
        ExportHandler(
            format=DataHandlerTypes.JINJA2,
            results=[
                {
                    "data": [
                        {
                            "name": db_table,
                            "report": report,
                            "summary": {
                                "rows": len(report),
                                "reportTitle": report_title,
                                "stats": stats,
                            },
                        }
                    ]
                }
            ],
            template_path=template_path,
            file_path=file_path,
        ).export()
    else:
        logging.warn("No report results found.")


@app.command(name="csv", no_args_is_help=True, help="Generate CSV Report")
def csv_handler(
    ctx: typer.Context,
    # current compliance reports do not support time range queries
    # start_time: datetime = typer.Option(
    #     (datetime.utcnow() - timedelta(hours=25)).strftime(common.ISO_FORMAT),
    #     formats=[common.ISO_FORMAT],
    #     help="Start time for query period",
    # ),
    # end_time: datetime = typer.Option(
    #     (datetime.utcnow()).strftime(common.ISO_FORMAT),
    #     formats=[common.ISO_FORMAT],
    #     help="End time for query period",
    # ),
    aws_compliance: AWSComplianceTypes = typer.Option(
        AWSComplianceTypes.AWS_CIS_S3.value,
        help="AWS compliance framework",
    ),
    gcp_compliance: GCPComplianceTypes = typer.Option(
        GCPComplianceTypes.GCP_CIS12.value,
        help="GCP compliance framework",
    ),
    azure_compliance: AzureComplianceTypes = typer.Option(
        AzureComplianceTypes.AZURE_CIS_131.value,
        help="Azure compliance framework",
    ),
    organization: Optional[str] = typer.Option(
        None,
        help="GCP organization id; Required when org level integration is not used",
    ),
    subaccounts: bool = typer.Option(
        False,
        help="Enumerate subaccounts",
        envvar=common.LACEWORK_REPORTS_SUBACCOUNTS,
    ),
    summary_only: bool = typer.Option(
        False,
        help="Return only summary details",
        envvar=common.LACEWORK_REPORTS_SUBACCOUNTS,
    ),
    file_path: str = typer.Option(
        ...,
        help="Path to exported result",
        envvar=common.LACEWORK_REPORTS_FILE_PATH,
    ),
    ignore_errors: bool = typer.Option(
        True,
        help="Ignore error for missing reports or inaccessible account details.",
    ),
) -> None:
    """
    Set the command context
    """
    missing_cloud_accounts = []

    # connect lacework client
    lw = common.config.connect()

    # report details
    db_table = "compliance_coverage"

    reportHelper = ReportHelper()
    db_path = Path("database.db")
    # db_path.unlink(missing_ok=True)
    db_connection = f"sqlite:///{db_path.absolute()}?check_same_thread=False"

    reportHelper.sqlite_drop_table(db_table, db_connection)
    reportHelper.sqlite_drop_table("cloud_accounts", db_connection)

    has_subaccounts = False
    if subaccounts:
        lwAccounts = reportHelper.get_subaccounts(client=lw)
        if len(lwAccounts) == 0:
            logging.error("Subaccounts specificed but none found")
            raise Exception("Subaccounts specificed but none found")
        else:
            has_subaccounts = True
    else:
        lwAccounts = [{"accountName": lw._account}]

    for lwAccount in lwAccounts:
        if has_subaccounts:
            logging.info(f"Switching to subaccount context: {lwAccount['accountName']}")
            lw.set_subaccount(lwAccount["accountName"])

        # get cloud accounts and sync to sqlite
        cloud_accounts = reportHelper.get_cloud_accounts(
            client=lw, lwAccount=lwAccount["accountName"], organization=organization
        )
        ExportHandler(
            format=DataHandlerTypes.SQLITE,
            results=[{"data": cloud_accounts}],
            file_path=file_path,
            db_table="cloud_accounts",
            db_connection=db_connection,
        ).export()

        for cloud_account in cloud_accounts:
            logging.info(
                f"Enumerating {lwAccount['accountName']}:{cloud_account['accountId']}"
            )
            report = reportHelper.get_compliance_report(
                client=lw,
                cloud_account=cloud_account["accountId"],
                lwAccount=lwAccount["accountName"],
                aws_compliance=aws_compliance,
                gcp_compliance=gcp_compliance,
                azure_compliance=azure_compliance,
                ignore_errors=ignore_errors,
                organization=organization,
            )

            if len(report) > 0:
                ExportHandler(
                    format=DataHandlerTypes.SQLITE,
                    results=[{"data": report}],
                    file_path=file_path,
                    db_table=db_table,
                    db_connection=db_connection,
                ).export()
            else:
                missing_cloud_accounts.append(cloud_account["accountId"])

    for miss in missing_cloud_accounts:
        logging.warn(f"missing report for : {miss}")

    # use sqlite query to generate final result
    results = reportHelper.sqlite_queries(
        queries=ComplianceQueries, db_table=db_table, db_connection=db_connection
    )

    if len(results["report"]) > 0:
        if summary_only:
            report = results["account_coverage"]
        else:
            report = results["report"]

        logging.info("Building CSV from resultant data...")
        ExportHandler(
            format=DataHandlerTypes.CSV,
            results=[{"data": report}],
            file_path=file_path,
        ).export()
    else:
        logging.warn("No report results found.")


if __name__ == "__main__":
    app()
