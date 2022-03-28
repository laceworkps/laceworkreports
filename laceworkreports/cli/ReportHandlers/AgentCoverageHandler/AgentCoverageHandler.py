"""
Report Handler
"""

import typing
from typing import Any

import logging
from datetime import datetime, timedelta
from pathlib import Path

import typer

from laceworkreports import common
from laceworkreports.sdk.DataHandlers import (
    DataHandlerTypes,
    ExportHandler,
    QueryHandler,
)
from laceworkreports.sdk.ReportHelpers import AgentQueries, ReportHelper

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
        Path(__file__).resolve().parent.joinpath("agent_coverage.html.j2"),
        help="Path to jinja2 template. Results will be passed as 'dataset' variable.",
        envvar=common.LACEWORK_REPORTS_TEMPLATE_PATH,
    ),
) -> None:
    """
    Set the command context
    """

    # connect the lacework client
    lw = common.config.connect()

    # report details
    report_title = "Agent Coverage"
    db_table = "agent_coverage"
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

    agents = []
    instances = []

    for account in accounts:
        if has_subaccounts:
            lw.set_subaccount(account["accountName"])

        # pull a list of ec2 instance details
        query_name = "EC2S"
        query_text = f"""{query_name}{{
                source {{LW_CFG_AWS_EC2_INSTANCES}}
                return {{RESOURCE_CONFIG, ACCOUNT_ID, RESOURCE_ID, RESOURCE_TYPE}}
                }}
                """

        query = ExportHandler(
            format=DataHandlerTypes.DICT,
            results=QueryHandler(
                client=lw,
                type=common.ObjectTypes.Queries.value,
                object=common.QueriesTypes.Execute.value,
                lql_query=query_text,
            ).execute(),
        ).export()

        # note: current limitation if 5000 rows
        logging.info(f"Found {len(query)} rows")
        if len(query) >= 5000:
            logging.warn("Max rows retrieved - results will be tructed beyond 5000")

        for h in query:
            name: Any = [
                item
                for item in h["RESOURCE_CONFIG"].get("Tags", {})
                if item["Key"] == "Name"
            ]

            if len(name) > 0:
                name = name.pop().get("Value")
            else:
                name = None

            data = {
                "name": name,
                "imageId": h["RESOURCE_CONFIG"].get("ImageId"),
                "instanceId": h["RESOURCE_CONFIG"].get("InstanceId"),
                "state": h["RESOURCE_CONFIG"].get("State").get("Name"),
                "accountId": f"aws:{h['ACCOUNT_ID']}",
                "lwAccount": account["accountName"],
            }
            instances.append(data)

        # pull a list of agent machine
        query_name = "AGENTS"
        query_text = f"""{query_name}{{
                source {{LW_HE_MACHINES}}
                filter {{TAGS:VmProvider = 'AWS'}}
                return {{TAGS}}
                }}
                """

        logging.info("Retrieving a list of lacework agents...")
        query = ExportHandler(
            format=DataHandlerTypes.DICT,
            results=QueryHandler(
                client=lw,
                type=common.ObjectTypes.Queries.value,
                object=common.QueriesTypes.Execute.value,
                lql_query=query_text,
            ).execute(),
        ).export()

        # note: current limitation if 5000 rows
        logging.info(f"Found {len(query)} rows")
        if len(query) >= 5000:
            logging.warn("Max rows retrieved - results will be tructed beyond 5000")

        for a in query:
            data = {
                "name": a["TAGS"].get("Name"),
                "imageId": a["TAGS"].get("ImageId"),
                "instanceId": a["TAGS"].get("InstanceId"),
                "state": "Unknown",
                "accountId": f"aws:{a['TAGS'].get('Account')}",
                "lwTokenShort": a["TAGS"].get("LwTokenShort"),
                "lwAccount": account["accountName"],
            }
            agents.append(data)

    logging.info("Building DICT from resultant data...")
    report = []

    logging.info("Adding instances with agent status")
    # instances check for agent
    for i in instances:
        has_lacework = False
        instanceId = i["instanceId"]
        record: typing.Any = [
            item for item in agents if item["instanceId"] == instanceId
        ]

        if len(record) > 0:
            record = record.pop().get("lwTokenShort")
        else:
            record = None

        if record is not None:
            has_lacework = True

        row = {
            "name": i["name"],
            "imageId": i["imageId"],
            "instanceId": i["instanceId"],
            "state": i["state"],
            "accountId": i["accountId"],
            "lacework": has_lacework,
            "hasEC2InstanceConfig": True,
            "lwTokenShort": record,
            "lwAccount": i["lwAccount"],
        }
        report.append(row)

    logging.info("Writing agents with no ec2 config")
    # agents installed but not in ec2 instances
    missing_count = 0
    for i in agents:
        has_ec2_instance = True
        instanceId = i["instanceId"]
        record = [item for item in instances if item["instanceId"] == instanceId]

        if len(record) > 0:
            record = record.pop().get("instanceId")
        else:
            record = None

        if record is not None:
            has_ec2_instance = True

        # if we have an agent but no ec2 log it
        if has_ec2_instance is False:
            missing_count += 1
            row = {
                "name": i["name"],
                "imageId": i["imageId"],
                "instanceId": i["instanceId"],
                "state": i["state"],
                "account": i["accountId"],
                "lwTokenShort": i["lwTokenShort"],
                "lwAccount": i["lwAccount"],
                "lacework": True,
                "hasEC2InstanceConfig": has_ec2_instance,
            }
            report.append(row)

    # sync to sqlite to build stats
    results = reportHelper.sqlite_sync_report(
        report=report, table_name=db_table, queries=AgentQueries
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
        logging.warn("No results found")

    if missing_count > 0:
        logging.warn(
            f"Found {missing_count} agents installed without associated config. Missing cloud config integration."
        )
    else:
        logging.info("WOO HOO! No agents found with missing config")


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
) -> None:
    """
    Set the command context
    """

    # connect the lacework client
    lw = common.config.connect()

    # report details
    db_table = "agent_coverage"
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

    agents = []
    instances = []

    for account in accounts:
        if has_subaccounts:
            lw.set_subaccount(account["accountName"])

        # pull a list of ec2 instance details
        query_name = "EC2S"
        query_text = f"""{query_name}{{
                source {{LW_CFG_AWS_EC2_INSTANCES}}
                return {{RESOURCE_CONFIG, ACCOUNT_ID, RESOURCE_ID, RESOURCE_TYPE}}
                }}
                """

        query = ExportHandler(
            format=DataHandlerTypes.DICT,
            results=QueryHandler(
                client=lw,
                type=common.ObjectTypes.Queries.value,
                object=common.QueriesTypes.Execute.value,
                lql_query=query_text,
            ).execute(),
        ).export()

        # note: current limitation if 5000 rows
        logging.info(f"Found {len(query)} rows")
        if len(query) >= 5000:
            logging.warn("Max rows retrieved - results will be tructed beyond 5000")

        for h in query:
            name: Any = [
                item
                for item in h["RESOURCE_CONFIG"].get("Tags", {})
                if item["Key"] == "Name"
            ]

            if len(name) > 0:
                name = name.pop().get("Value")
            else:
                name = None

            data = {
                "name": name,
                "imageId": h["RESOURCE_CONFIG"].get("ImageId"),
                "instanceId": h["RESOURCE_CONFIG"].get("InstanceId"),
                "state": h["RESOURCE_CONFIG"].get("State").get("Name"),
                "accountId": f"aws:{h['ACCOUNT_ID']}",
                "lwAccount": account["accountName"],
            }
            instances.append(data)

        # pull a list of agent machine
        query_name = "AGENTS"
        query_text = f"""{query_name}{{
                source {{LW_HE_MACHINES}}
                filter {{TAGS:VmProvider = 'AWS'}}
                return {{TAGS}}
                }}
                """

        logging.info("Retrieving a list of lacework agents...")
        query = ExportHandler(
            format=DataHandlerTypes.DICT,
            results=QueryHandler(
                client=lw,
                type=common.ObjectTypes.Queries.value,
                object=common.QueriesTypes.Execute.value,
                lql_query=query_text,
            ).execute(),
        ).export()

        # note: current limitation if 5000 rows
        logging.info(f"Found {len(query)} rows")
        if len(query) >= 5000:
            logging.warn("Max rows retrieved - results will be tructed beyond 5000")

        for a in query:
            data = {
                "name": a["TAGS"].get("Name"),
                "imageId": a["TAGS"].get("ImageId"),
                "instanceId": a["TAGS"].get("InstanceId"),
                "state": "Unknown",
                "accountId": f"aws:{a['TAGS'].get('Account')}",
                "lwTokenShort": a["TAGS"].get("LwTokenShort"),
                "lwAccount": account["accountName"],
            }
            agents.append(data)

    logging.info("Building DICT from resultant data...")
    report = []

    logging.info("Adding instances with agent status")
    # instances check for agent
    for i in instances:
        has_lacework = False
        instanceId = i["instanceId"]
        record: typing.Any = [
            item for item in agents if item["instanceId"] == instanceId
        ]

        if len(record) > 0:
            record = record.pop().get("lwTokenShort")
        else:
            record = None

        if record is not None:
            has_lacework = True

        row = {
            "name": i["name"],
            "imageId": i["imageId"],
            "instanceId": i["instanceId"],
            "state": i["state"],
            "account": i["accountId"],
            "lacework": has_lacework,
            "hasEC2InstanceConfig": True,
            "lwTokenShort": record,
            "lwAccount": i["lwAccount"],
        }
        report.append(row)

    logging.info("Writing agents with no ec2 config")
    # agents installed but not in ec2 instances
    missing_count = 0
    for i in agents:
        has_ec2_instance = True
        instanceId = i["instanceId"]
        record = [item for item in instances if item["instanceId"] == instanceId]

        if len(record) > 0:
            record = record.pop().get("instanceId")
        else:
            record = None

        if record is not None:
            has_ec2_instance = True

        # if we have an agent but no ec2 log it
        if has_ec2_instance is False:
            missing_count += 1
            row = {
                "name": i["name"],
                "imageId": i["imageId"],
                "instanceId": i["instanceId"],
                "state": i["state"],
                "account": i["account"],
                "lwTokenShort": i["lwTokenShort"],
                "lwAccount": i["lwAccount"],
                "lacework": True,
                "hasEC2InstanceConfig": has_ec2_instance,
            }
            report.append(row)

    # sync to sqlite to build stats
    results = reportHelper.sqlite_sync_report(
        report=report, table_name=db_table, queries=AgentQueries
    )

    if len(results["report"]) > 0:

        report = results["report"]
        if summary_only:
            report = results["account_coverage"]

        logging.info("Building CSV from resultant data...")
        ExportHandler(
            format=DataHandlerTypes.CSV,
            results=[{"data": report}],
            file_path=file_path,
        ).export()
    else:
        logging.warn("No results found")

    if missing_count > 0:
        logging.warn(
            f"Found {missing_count} agents installed without associated config. Missing cloud config integration."
        )
    else:
        logging.info("WOO HOO! No agents found with missing config")


if __name__ == "__main__":
    app()
