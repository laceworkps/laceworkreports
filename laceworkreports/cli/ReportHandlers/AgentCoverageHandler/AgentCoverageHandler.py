"""
Report Handler
"""

import typing
from typing import Any, Optional

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
        (datetime.utcnow() - timedelta(hours=25)).strftime(common.ISO_FORMAT),
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
    db_table = "discovered_machines"

    reportHelper = ReportHelper()
    db_path = Path("database.db")
    # db_path.unlink(missing_ok=True)
    db_connection = f"sqlite:///{db_path.absolute()}?check_same_thread=False"

    reportHelper.sqlite_drop_table(db_table, db_connection)
    reportHelper.sqlite_drop_table("machines", db_connection)
    reportHelper.sqlite_drop_table("cloud_accounts", db_connection)
    reportHelper.sqlite_drop_table("discovered_machines", db_connection)
    reportHelper.sqlite_drop_table("discovered_cloud_accounts", db_connection)

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

    lacework_account_count = 0
    for lwAccount in lwAccounts:
        lacework_account_count += 1
        if has_subaccounts:
            logging.info(f"Switching to subaccount context: {lwAccount['accountName']}")
            lw.set_subaccount(lwAccount["accountName"])

        # sync cloud accounts with deployed agents
        logging.info("Syncing cloud accounts with deployed agents")
        reportHelper.get_discovered_cloud_accounts(
            client=lw,
            lwAccount=lwAccount["accountName"],
            use_sqlite=True,
            db_table="discovered_cloud_accounts",
            db_connection=db_connection,
        )
        try:
            result = reportHelper.sqlite_queries(
                queries={
                    "cloud_account_query": """
                                    SELECT 
                                        DISTINCT ACCOUNTID
                                    FROM 
                                        :db_table
                                    WHERE 
                                        ACCOUNTID IS NOT NULL
                                    """
                },
                db_connection=db_connection,
                db_table="discovered_cloud_accounts",
            )
            discovered_cloud_accounts = [
                x["ACCOUNTID"] for x in result["cloud_account_query"]
            ]
        except Exception:
            discovered_cloud_accounts = []

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

        logging.info(
            f"Discovered {len(discovered_cloud_accounts)} cloud accounts with agents deployed: {discovered_cloud_accounts}"
        )

        for cloud_account in cloud_accounts:

            if (
                cloud_account["enabled"] == 1
                and cloud_account["accountId"] in discovered_cloud_accounts
            ):
                # sync machines for this cloud account
                logging.info(
                    f"Syncing machines for {lwAccount['accountName']}:{cloud_account['accountId']}"
                )

                # sync all machines with agents
                reportHelper.get_active_machines(
                    client=lw,
                    lwAccount=lwAccount["accountName"],
                    cloud_account=cloud_account["accountId"],
                    use_sqlite=True,
                    db_table="machines",
                    db_connection=db_connection,
                )

                # sync all discovered ec2 and gce instances
                reportHelper.get_discovered_machines(
                    client=lw,
                    lwAccount=lwAccount["accountName"],
                    cloud_account=cloud_account["accountId"],
                    use_sqlite=True,
                    db_table="discovered_machines",
                    db_connection=db_connection,
                )

            else:
                logging.info(
                    f"Skipping disabled or inactive account {lwAccount['accountName']}:{cloud_account['accountId']}"
                )

    # use sqlite query to generate final result
    results = reportHelper.sqlite_queries(
        queries=AgentQueries, db_table=db_table, db_connection=db_connection
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


@app.command(name="csv", no_args_is_help=True, help="Generate CSV Report")
def csv_handler(
    ctx: typer.Context,
    start_time: datetime = typer.Option(
        (datetime.utcnow() - timedelta(hours=25)).strftime(common.ISO_FORMAT),
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
            lw.set_subaccount(lwAccount["accountName"])

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
                "lwAccount": lwAccount["accountName"],
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
                "lwAccount": lwAccount["accountName"],
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
