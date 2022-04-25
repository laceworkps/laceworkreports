"""
Report Handler
"""
import typing

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

from .VpcChartHelper import build_target_vpc_output

app = typer.Typer(no_args_is_help=True)


@app.command(no_args_is_help=True, help="Generate VPC Report PNG and Associated CSV")
def png(
    ctx: typer.Context,
    start_time: datetime = typer.Option(
        (datetime.utcnow() - timedelta(days=7)).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
        help="Start time for query period",
    ),
    end_time: datetime = typer.Option(
        (datetime.utcnow()).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
        help="End time for query period",
    ),
    output_directory: str = typer.Option(
        Path("./out").resolve(),
        help="Path to exported resutls. Default current directory",
    ),
    vpcs_list: str = typer.Option(
        ...,
        help="Comma separated list of VPCs to processt or file with newline seperated values . Use @ prefex to specify file path",
    ),
) -> None:
    # connect the lacework client
    lw = common.config.connect()

    # try:
    if not Path(output_directory).resolve().exists():
        logging.warn(
            f"output_directory path does not exist. path will be created: {Path(output_directory).resolve()}"
        )
        Path(output_directory).mkdir(parents=True, exist_ok=True)

    vpcs = []
    if vpcs_list[0] == "@":
        if not Path(vpcs_list[1:]).exists():
            raise Exception("vpcs_list path does not exist")
        try:
            vpcs = [x.strip() for x in Path(vpcs_list[1:]).read_text().splitlines()]
        except Exception as e:
            raise typer.BadParameter(f"Failed to parse vpcs_list: {e}")
    else:
        try:
            vpcs = [x.strip() for x in vpcs_list.split(",")]
        except Exception as e:
            raise typer.BadParameter(f"Failed to parse vpcs_list json: {e}")

    vpcs_query = """VPCS {
        source {
            LW_CFG_AWS_EC2_VPCS
        }
        return {
            RESOURCE_ID, 
            RESOURCE_CONFIG, 
            ARN, 
            RESOURCE_REGION, 
            ACCOUNT_ID, 
            ACCOUNT_ALIAS, 
            RESOURCE_TAGS
        }
    }"""

    nodes = ExportHandler(
        format=DataHandlerTypes.DICT,
        results=QueryHandler(
            client=lw,
            type=common.ObjectTypes.Queries.value,
            object=common.QueriesTypes.Execute.value,
            start_time=start_time,
            end_time=end_time,
            lql_query=vpcs_query,
        ).execute(),
        field_map={
            "RESOURCE_ID": "RESOURCE_ID",
            "RESOURCE_CONFIG": "RESOURCE_CONFIG",
            "ACCOUNT_ID": "ACCOUNT_ID",
            "ACCOUNT_ALIAS": "ACCOUNT_ALIAS",
            "RESOURCE_REGION": "RESOURCE_REGION",
            "ARN": "ARN",
            "RESOURCE_TAGS": "RESOURCE_TAGS",
        },
    ).export()

    vpc_service_connection_query = """VPC_SERVICE_CONNECTIONS {
        source {
            LW_CFG_AWS_EC2_VPC_PEERING_CONNECTIONS
        }
        return {
            RESOURCE_ID, 
            RESOURCE_CONFIG,
            ARN,
            RESOURCE_REGION,
            ACCOUNT_ID,
            ACCOUNT_ALIAS,
            RESOURCE_TAGS
        }
    }"""

    edges = ExportHandler(
        format=DataHandlerTypes.DICT,
        results=QueryHandler(
            client=lw,
            type=common.ObjectTypes.Queries.value,
            object=common.QueriesTypes.Execute.value,
            start_time=start_time,
            end_time=end_time,
            lql_query=vpc_service_connection_query,
        ).execute(),
        field_map={
            "RESOURCE_ID": "RESOURCE_ID",
            "RESOURCE_CONFIG": "RESOURCE_CONFIG",
            "ACCOUNT_ID": "ACCOUNT_ID",
            "ACCOUNT_ALIAS": "ACCOUNT_ALIAS",
            "RESOURCE_REGION": "RESOURCE_REGION",
            "ARN": "ARN",
            "RESOURCE_TAGS": "RESOURCE_TAGS",
        },
    ).export()

    for vpc in vpcs:
        build_target_vpc_output(vpc, nodes, edges, output_directory)

    logging.info("Operation Completed Successfully!")
    # except Exception as e:
    #     logging.error(e)


if __name__ == "__main__":
    app()
