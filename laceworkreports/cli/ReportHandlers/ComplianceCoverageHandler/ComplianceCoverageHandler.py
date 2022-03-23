"""
Report Handler
"""

from typing import Optional

import csv
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import jinja2
import typer

from laceworkreports import common
from laceworkreports.sdk.ReportHelpers import get_cloud_accounts, sqlite_sync_report

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
) -> None:
    """
    Set the command context
    """

    # connect lacework client
    lw = common.config.connect()

    # report details
    report_title = "Compliance Coverage"
    db_table = "compliance_coverage"

    # get cloud account list - enumerate accounts and pull report
    cloud_accounts = get_cloud_accounts(client=lw)

    reports = []
    for account in cloud_accounts:
        if account["type"] == "AwsCfg" and account["enabled"] == 1:
            report = lw.compliance.get_latest_aws_report(
                aws_account_id=account["account"], file_format="json", report_type=None
            )
            reports.append(report["data"].pop())
        elif account["type"] == "GcpCfg" and account["enabled"] == 1:
            # requires special case handling as there are cases where orgId is not available via API
            orgId = None
            if organization is None and account["orgId"] is None:
                logging.critical(
                    "No GCP organization available and not specified (specify --organization)"
                )
                raise Exception(
                    "No GCP organization available and not specified (specify --organization)"
                )
            # when org is available use it
            elif organization is not None and account["orgId"] is not None:
                orgId = account["orgId"]
            else:
                orgId = organization

            for projectId in account["projectIds"]:
                report = lw.compliance.get_latest_gcp_report(
                    gcp_organization_id=orgId,
                    gcp_project_id=projectId,
                    file_format="json",
                    report_type=None,
                )
                reports.append(report["data"].pop())
        elif account["type"] == "AzureCfg" and account["enabled"] == 1:
            for subscriptionId in account["subscriptionIds"]:
                report = lw.compliance.get_latest_azure_report(
                    azure_tenant_id=account["tenantId"],
                    azure_subscription_id=subscriptionId,
                    file_format="json",
                    report_type=None,
                )
                reports.append(report["data"].pop())

    # report queries
    queries = {
        "report": """
                    select 
                        reportType,
                        reportTime,
                        reportTitle,
                        accountId,
                        organizationId,
                        projectId,
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
                        json_extract(json_recommendations.value, '$.SEVERITY') AS severity,
                        CAST(100-cast(json_array_length(json_extract(json_recommendations.value, '$.VIOLATIONS')) AS FLOAT)*100/json_extract(json_recommendations.value, '$.RESOURCE_COUNT') AS INTEGER) as percent
                    from 
                        :table_name, 
                        json_each(:table_name.recommendations) AS json_recommendations
                    where
                        percent < 100 AND status != 'Compliant'
                    order by
                        accountId,
                        organizationId,
                        reportType,
                        rec_id
                    """,
        "account_coverage": """
                            SELECT 
                                t.Account,
                                CAST(AVG(t.Percent) AS INTEGER) AS Percent
                            FROM
                                (SELECT
                                    accountId AS Account,
                                    CAST(100-cast(json_array_length(json_extract(json_recommendations.value, '$.VIOLATIONS')) AS FLOAT)*100/json_extract(json_recommendations.value, '$.RESOURCE_COUNT') AS INTEGER) as percent
                                FROM
                                    :table_name,
                                    json_each(:table_name.recommendations) AS json_recommendations
                                ) as t
                            GROUP BY
                                Account
                            ORDER BY
                                Account,
                                Percent
                            """,
        "total_coverage": """
                            SELECT 
                                CAST(AVG(t.Percent) AS INTEGER) AS Percent
                            FROM
                                (SELECT
                                    accountId AS Account,
                                    CAST(100-cast(json_array_length(json_extract(json_recommendations.value, '$.VIOLATIONS')) AS FLOAT)*100/json_extract(json_recommendations.value, '$.RESOURCE_COUNT') AS INTEGER) as percent
                                FROM
                                    :table_name,
                                    json_each(:table_name.recommendations) AS json_recommendations
                                ) as t
                            """,
        "total_accounts": """
                            SELECT
                                COUNT(DISTINCT accountId) AS Total
                            FROM
                                :table_name
                            """,
    }

    # use sqlite query to generate final result
    results = sqlite_sync_report(report=reports, table_name=db_table, queries=queries)
    report = results["report"]
    stats = {}
    stats["account_coverage"] = results["account_coverage"]
    stats["total_coverage"] = results["total_coverage"]
    stats["total_accounts"] = results["total_accounts"]

    # write jinja template
    report_template = template_path
    fileloader = jinja2.FileSystemLoader(searchpath=os.path.dirname(report_template))
    env = jinja2.Environment(
        loader=fileloader, extensions=["jinja2.ext.do"], autoescape=True
    )
    template = env.get_template(os.path.basename(report_template))

    template_result = template.render(
        datasets=[
            {
                "name": db_table,
                "report": report,
                "summary": {
                    "rows": len(report),
                    "reportTitle": report_title,
                    "stats": stats,
                },
            }
        ],
        datetime=datetime,
        timedelta=timedelta,
        config=common.config,
    )

    Path(file_path).write_text(template_result)


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
    file_path: str = typer.Option(
        ...,
        help="Path to exported result",
        envvar=common.LACEWORK_REPORTS_FILE_PATH,
    ),
) -> None:
    """
    Set the command context
    """

    # connect lacework client
    lw = common.config.connect()

    # report details
    db_table = "compliance_coverage"

    # get cloud account list - enumerate accounts and pull report
    cloud_accounts = get_cloud_accounts(client=lw)

    # build list of unique cloud account
    reports = []
    for account in cloud_accounts:
        if account["type"] == "AwsCfg" and account["enabled"] == 1:
            report = lw.compliance.get_latest_aws_report(
                aws_account_id=account["account"], file_format="json", report_type=None
            )
            reports.append(report["data"].pop())
        elif account["type"] == "GcpCfg" and account["enabled"] == 1:
            # requires special case handling as there are cases where orgId is not available via API
            orgId = None
            if organization is None and account["orgId"] is None:
                logging.critical(
                    "No GCP organization available and not specified (specify --organization)"
                )
                raise Exception(
                    "No GCP organization available and not specified (specify --organization)"
                )
            # when org is available use it
            elif organization is not None and account["orgId"] is not None:
                orgId = account["orgId"]
            else:
                orgId = organization

            for projectId in account["projectIds"]:
                report = lw.compliance.get_latest_gcp_report(
                    gcp_organization_id=orgId,
                    gcp_project_id=projectId,
                    file_format="json",
                    report_type=None,
                )
                reports.append(report["data"].pop())
        elif account["type"] == "AzureCfg" and account["enabled"] == 1:
            for subscriptionId in account["subscriptionIds"]:
                report = lw.compliance.get_latest_azure_report(
                    azure_tenant_id=account["tenantId"],
                    azure_subscription_id=subscriptionId,
                    file_format="json",
                    report_type=None,
                )
                reports.append(report["data"].pop())

    # report queries
    queries = {
        "report": f"""
                    select 
                        reportType,
                        reportTime,
                        reportTitle,
                        accountId,
                        organizationId,
                        projectId,
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
                        json_extract(json_recommendations.value, '$.SEVERITY') AS severity,
                        CAST(100-cast(json_array_length(json_extract(json_recommendations.value, '$.VIOLATIONS')) AS FLOAT)*100/json_extract(json_recommendations.value, '$.RESOURCE_COUNT') AS INTEGER) as percent
                    from 
                        {db_table}, 
                        json_each({db_table}.recommendations) AS json_recommendations
                    where
                        percent < 100 AND status != 'Compliant'
                    order by
                        accountId,
                        organizationId,
                        reportType,
                        rec_id
                    """,
    }

    # use sqlite query to generate final result
    results = sqlite_sync_report(report=reports, table_name=db_table, queries=queries)
    report = results["report"]

    # write results to csv
    header = False
    with open(file_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)

        for row in report:
            if not header:
                writer.writerow(row.keys())
                header = True

            writer.writerow(row.values())


if __name__ == "__main__":
    app()
