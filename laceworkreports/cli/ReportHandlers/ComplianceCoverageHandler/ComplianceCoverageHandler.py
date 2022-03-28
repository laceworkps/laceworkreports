"""
Report Handler
"""

from typing import Optional

import logging
from datetime import datetime, timedelta
from pathlib import Path

import typer

from laceworkreports import common
from laceworkreports.sdk.DataHandlers import DataHandlerTypes, ExportHandler
from laceworkreports.sdk.ReportHelpers import ComplianceQueries, ReportHelper

app = typer.Typer(no_args_is_help=True)


@app.command(no_args_is_help=True, help="Generate HTML report")
def html(
    ctx: typer.Context,
    start_time: datetime = typer.Option(
        (datetime.utcnow() - timedelta(days=1)).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
        help="Start time for query period",
    ),
    end_time: datetime = typer.Option(
        (datetime.utcnow()).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
        help="End time for query period",
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

    # connect lacework client
    lw = common.config.connect()

    # report details
    report_title = "Compliance Coverage"
    db_table = "compliance_coverage"
    reportHelper = ReportHelper()

    has_subaccounts = False
    if subaccounts:
        accounts = reportHelper.get_subaccounts(client=lw)
        if len(accounts) == 0:
            logging.error("Subaccounts specificed but none found")
            raise Exception("Subaccounts specificed but none found")
        else:
            has_subaccounts = True
    else:
        accounts = [{"accountName": lw._account}]

    lacework_account_count = 0
    cloud_account_count = 0
    missing_cloud_accounts = []
    reports = []
    for account in accounts:
        lacework_account_count += 1
        if has_subaccounts:
            logging.info(f"Switching to subaccount context: {account['accountName']}")
            lw.set_subaccount(account["accountName"])

        for cloud_account in reportHelper.get_cloud_accounts(client=lw):
            # get a list of formatted enabled cloud accounts
            for ca in reportHelper.cloud_accounts_format(
                cloud_account=cloud_account, organization=organization
            ):
                cloud_account_count += 1
                logging.info(f"Enumerating {account['accountName']}:{ca}")
                report = reportHelper.get_compliance_report(
                    client=lw,
                    cloud_account=ca,
                    account=account,
                    ignore_errors=ignore_errors,
                    organization=organization,
                )

                if len(report) > 0:
                    reports.append(report[0])
                else:
                    missing_cloud_accounts.append(ca)

    for miss in missing_cloud_accounts:
        logging.warn(f"missing report for : {miss}")

    # use sqlite query to generate final result
    results = reportHelper.sqlite_sync_report(
        report=reports,
        table_name=db_table,
        queries=ComplianceQueries,
        # bypass temporary storage - testing only
        # db_path_override="database.db",
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
    start_time: datetime = typer.Option(
        (datetime.utcnow() - timedelta(days=1)).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
        help="Start time for query period",
    ),
    end_time: datetime = typer.Option(
        (datetime.utcnow()).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
        help="End time for query period",
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

    # connect lacework client
    lw = common.config.connect()

    # report details
    db_table = "compliance_coverage"
    reportHelper = ReportHelper()

    has_subaccounts = False
    if subaccounts:
        accounts = reportHelper.get_subaccounts(client=lw)
        if len(accounts) == 0:
            logging.error("Subaccounts specificed but none found")
            raise Exception("Subaccounts specificed but none found")
        else:
            has_subaccounts = True
    else:
        accounts = [{"accountName": lw._account}]

    lacework_account_count = 0
    cloud_account_count = 0
    missing_cloud_accounts = []
    reports = []
    for account in accounts:
        lacework_account_count += 1
        if has_subaccounts:
            logging.info(f"Switching to subaccount context: {account['accountName']}")
            lw.set_subaccount(account["accountName"])

        for cloud_account in reportHelper.get_cloud_accounts(client=lw):
            # get a list of formatted enabled cloud accounts
            for ca in reportHelper.cloud_accounts_format(
                cloud_account=cloud_account, organization=organization
            ):
                cloud_account_count += 1
                logging.info(f"Enumerating {account['accountName']}:{ca}")
                report = reportHelper.get_compliance_report(
                    client=lw,
                    cloud_account=ca,
                    account=account,
                    ignore_errors=ignore_errors,
                    organization=organization,
                )

                if len(report) > 0:
                    reports.append(report[0])
                else:
                    missing_cloud_accounts.append(ca)

    for miss in missing_cloud_accounts:
        logging.warn(f"missing report for : {miss}")

    # use sqlite query to generate final result
    results = reportHelper.sqlite_sync_report(
        report=reports,
        table_name=db_table,
        queries=ComplianceQueries,
        # bypass temporary storage - testing only
        db_path_override="test.db",
    )

    if len(results["report"]) > 0:
        if summary_only:
            report = results["account_coverage_severity"]
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
