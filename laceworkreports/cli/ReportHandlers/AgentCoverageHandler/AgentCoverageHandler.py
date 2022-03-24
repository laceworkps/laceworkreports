"""
Report Handler
"""

from typing import Any, Dict

import csv
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import jinja2
import typer

from laceworkreports import common
from laceworkreports.sdk.DataHandlers import (
    DataHandlerTypes,
    ExportHandler,
    QueryHandler,
)
from laceworkreports.sdk.ReportHelpers import sqlite_sync_report

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

    instances = []

    # note: current limitation if 5000 rows
    logging.info(f"Found {len(query)} rows")
    if len(query) >= 5000:
        logging.warn("Max rows retrieved - results will be tructed beyond 5000")

    for h in query:
        name: dict[Any, Any] = [
            item
            for item in h["RESOURCE_CONFIG"].get("Tags", {})
            if item["Key"] == "Name"
        ]

        if len(name) > 0:
            name = name.pop().get("Value")
        else:
            name = None

        data = {
            "Name": name,
            "ImageId": h["RESOURCE_CONFIG"].get("ImageId"),
            "InstanceId": h["RESOURCE_CONFIG"].get("InstanceId"),
            "State": h["RESOURCE_CONFIG"].get("State").get("Name"),
            "Account": h["ACCOUNT_ID"],
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

    agents: list[Any] = []
    for a in query:
        data = {
            "Name": a["TAGS"].get("Name"),
            "ImageId": a["TAGS"].get("ImageId"),
            "InstanceId": a["TAGS"].get("InstanceId"),
            "State": "Unknown",
            "Account": a["TAGS"].get("Account"),
            "LwTokenShort": a["TAGS"].get("LwTokenShort"),
        }
        agents.append(data)

    logging.info("Building DICT from resultant data...")
    report = []

    logging.info("Adding instances with agent status")
    # instances check for agent
    for i in instances:
        has_lacework = False
        InstanceId = i["InstanceId"]
        record = [item for item in agents if item["InstanceId"] == InstanceId]

        if len(record) > 0:
            record = record.pop().get("LwTokenShort")
        else:
            record = None

        if record is not None:
            has_lacework = True

        row = {
            "Name": i["Name"],
            "ImageId": i["ImageId"],
            "InstanceId": i["InstanceId"],
            "State": i["State"],
            "Account": i["Account"],
            "Lacework": has_lacework,
            "HasEC2InstanceConfig": True,
            "LwTokenShort": record,
        }
        report.append(row)

    logging.info("Writing agents with no ec2 config")
    # agents installed but not in ec2 instances
    missing_count = 0
    for i in agents:
        has_ec2_instance = True
        InstanceId = i["InstanceId"]
        record = [item for item in instances if item["InstanceId"] == InstanceId]

        if len(record) > 0:
            record = record.pop().get("InstanceId")
        else:
            record = None

        if record is not None:
            has_ec2_instance = True

        # if we have an agent but no ec2 log it
        if has_ec2_instance is False:
            missing_count += 1
            row = {
                "Name": i["Name"],
                "ImageId": i["ImageId"],
                "InstanceId": i["InstanceId"],
                "State": i["State"],
                "Account": i["Account"],
                "LwTokenShort": i["LwTokenShort"],
                "Lacework": True,
                "HasEC2InstanceConfig": has_ec2_instance,
            }
            report.append(row)

    if missing_count > 0:
        logging.warn(
            f"Found {missing_count} agents installed without associated config. Missing cloud config integration."
        )
    else:
        logging.info("WOO HOO! No agents found with missing config")

    # sync to sqlite to build stats
    queries = {
        "account_coverage": """
                            SELECT 
                                Account, 
                                SUM(Lacework) AS Installed,
                                COUNT(*) AS Total,
                                SUM(Lacework)*100/COUNT(*) AS Percent
                            FROM 
                                :table_name 
                            WHERE
                                State = 'running'
                            GROUP BY
                                Account
                            ORDER BY
                                Account,
                                Percent
                            """,
        "total_coverage": """
                            SELECT  
                                SUM(Lacework) AS Installed,
                                COUNT(*)-SUM(Lacework) AS NotInstalled,
                                COUNT(*) AS Total,
                                SUM(Lacework)*100/COUNT(*) AS Percent
                            FROM 
                                :table_name 
                            WHERE
                                State = 'running'
                            """,
        "total_accounts": """
                            SELECT  
                                COUNT(DISTINCT ACCOUNT) AS Total
                            FROM 
                                :table_name
                            """,
    }
    results = sqlite_sync_report(report=report, table_name=db_table, queries=queries)
    stats = {}
    stats["account_coverage"] = results["account_coverage"]
    stats["total_coverage"] = results["total_coverage"]
    stats["total_accounts"] = results["total_accounts"]

    report_template = template_path
    fileloader = jinja2.FileSystemLoader(searchpath=os.path.dirname(report_template))
    env = jinja2.Environment(
        loader=fileloader, extensions=["jinja2.ext.do"], autoescape=True
    )
    template = env.get_template(os.path.basename(report_template))

    template_result = template.render(
        datasets=[
            {
                "name": "agent_deployment",
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
    file_path: str = typer.Option(
        ...,
        help="Path to exported result",
        envvar=common.LACEWORK_REPORTS_FILE_PATH,
    ),
) -> None:
    """
    Set the command context
    """
    lw = common.config.connect()

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

    instances: list[Any] = []

    # note: current limitation if 5000 rows
    logging.info(f"Found {len(query)} rows")
    if len(query) >= 5000:
        logging.warn("Max rows retrieved - results will be tructed beyond 5000")

    for h in query:
        name: dict[Any, Any] = [
            item
            for item in h["RESOURCE_CONFIG"].get("Tags", {})
            if item["Key"] == "Name"
        ].pop()

        data = {
            "Name": name.get("Value"),
            "ImageId": h["RESOURCE_CONFIG"].get("ImageId"),
            "InstanceId": h["RESOURCE_CONFIG"].get("InstanceId"),
            "State": h["RESOURCE_CONFIG"].get("State").get("Name"),
            "Account": h["ACCOUNT_ID"],
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

    agents = []
    for a in query:
        data = {
            "Name": a["TAGS"].get("Name"),
            "ImageId": a["TAGS"].get("ImageId"),
            "InstanceId": a["TAGS"].get("InstanceId"),
            "State": "Unknown",
            "Account": a["TAGS"].get("Account"),
            "LwTokenShort": a["TAGS"].get("LwTokenShort"),
        }
        agents.append(data)

    logging.info("Building CSV from resultant data...")
    header = False
    with open(file_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)

        logging.info("Writing instances with agent status")
        # instances check for agent
        for i in instances:
            has_lacework = False
            InstanceId = i["InstanceId"]
            record = [item for item in agents if item["InstanceId"] == InstanceId].pop()

            if record.get("LwTokenShort") is not None:
                has_lacework = True

            row = {
                "Name": i["Name"],
                "ImageId": i["ImageId"],
                "InstanceId": i["InstanceId"],
                "State": i["State"],
                "Account": i["Account"],
                "Lacework": has_lacework,
                "HasEC2InstanceConfig": True,
                "LwTokenShort": record.get("LwTokenShort"),
            }
            if not header:
                writer.writerow(row.keys())
                header = True

            writer.writerow(row.values())

        logging.info("Writing agents with no ec2 config")
        # agents installed but not in ec2 instances
        missing_count = 0
        for i in agents:
            has_ec2_instance = True
            InstanceId = i["InstanceId"]
            record = [
                item for item in instances if item["InstanceId"] == InstanceId
            ].pop()

            if record.get("InstanceId") is not None:
                has_ec2_instance = True

            # if we have an agent but no ec2 log it
            if has_ec2_instance is False:
                missing_count += 1
                row = {
                    "Name": i["Name"],
                    "ImageId": i["ImageId"],
                    "InstanceId": i["InstanceId"],
                    "State": i["State"],
                    "Account": i["Account"],
                    "LwTokenShort": i["LwTokenShort"],
                    "Lacework": True,
                    "HasEC2InstanceConfig": has_ec2_instance,
                }
                if not header:
                    writer.writerow(row.keys())
                    header = True

                writer.writerow(row.values())

    if missing_count > 0:
        logging.warn(
            f"Found {missing_count} agents installed without associated config. Missing cloud config integration."
        )
    else:
        logging.info("WOO HOO! No agents found with missing config")


if __name__ == "__main__":
    app()
