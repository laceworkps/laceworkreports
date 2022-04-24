"""
Report Handler
"""
from typing import Optional

import csv
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import matplotlib.pyplot as plt
import networkx
import typer
from laceworksdk import LaceworkClient

from laceworkreports import common
from laceworkreports.sdk.DataHandlers import DataHandlerTypes, ExportHandler
from laceworkreports.sdk.ReportHelpers import ComplianceQueries, ReportHelper


class LQL_Resource:
    def __init__(
        self,
        resource_id,
        resource_config,
        account_id,
        account_alias,
        resource_region,
        arn,
        tags,
    ):
        self.resource_id = resource_id
        self.resource_config = resource_config
        self.account_id = account_id
        self.account_alias = account_alias
        self.resource_region = resource_region
        self.arn = arn
        self.tags = tags

    def __str__(self):
        return f"{self.resource_id} -- {self.resource_config}"

    def __repr__(self) -> str:
        return self.__str__()


def execute_implicit_query(lw_client, query):
    # Build start/end times
    current_time = datetime.now(timezone.utc)
    start_time = current_time - timedelta(days=7)
    start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = current_time
    end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    logging.debug(f"LQL Query is: {query}")

    response = lw_client.queries.execute(
        evaluator_id="<<IMPLICIT>>",
        query_text=query,
        arguments={
            "StartTimeRange": start_time,
            "EndTimeRange": end_time,
        },
    )

    data = response.get("data", [])
    logging.debug(f"LQL Response: {data}")
    return data


def load_vpcs(lw_client):
    vpcs_query_text = """VPCS {
        source {
           LW_CFG_AWS_EC2_VPCS
        }
        return {RESOURCE_ID, RESOURCE_CONFIG, ARN, RESOURCE_REGION, ACCOUNT_ID, ACCOUNT_ALIAS, RESOURCE_TAGS}
    }"""

    vpcs_data = execute_implicit_query(lw_client, vpcs_query_text)
    vpcs = []
    for vpc in vpcs_data:
        lql_resource = LQL_Resource(
            vpc["RESOURCE_ID"],
            vpc["RESOURCE_CONFIG"],
            vpc["ACCOUNT_ID"],
            vpc["ACCOUNT_ALIAS"],
            vpc["RESOURCE_REGION"],
            vpc["ARN"],
            vpc["RESOURCE_TAGS"],
        )
        vpcs.append(lql_resource)

    return vpcs


def load_service_connections(lw_client):
    vpc_service_connection_query_text = """VPC_SERVICE_CONNECTIONS {
        source {
          LW_CFG_AWS_EC2_VPC_PEERING_CONNECTIONS
        }
        return {RESOURCE_ID, RESOURCE_CONFIG, ARN, RESOURCE_REGION, ACCOUNT_ID, ACCOUNT_ALIAS, RESOURCE_TAGS}
    }"""

    vpcs_service_connection_data = execute_implicit_query(
        lw_client, vpc_service_connection_query_text
    )
    vpc_service_connections = []
    for vpc_service_connection in vpcs_service_connection_data:
        lql_resource = LQL_Resource(
            vpc_service_connection["RESOURCE_ID"],
            vpc_service_connection["RESOURCE_CONFIG"],
            vpc_service_connection["ACCOUNT_ID"],
            vpc_service_connection["ACCOUNT_ALIAS"],
            vpc_service_connection["RESOURCE_REGION"],
            vpc_service_connection["ARN"],
            vpc_service_connection["RESOURCE_TAGS"],
        )
        vpc_service_connections.append(lql_resource)

    return vpc_service_connections


def build_graph(nodes, edges):
    Graph = networkx.Graph()
    labeldict = {}

    values = set()
    for node in nodes:
        Graph.add_node(node.resource_id)
        label_val = (
            node.tags["Name"]
            if "Name" in node.tags and node.tags["Name"] != ""
            else node.resource_id
        )
        # handle cases with duplicate labels, fallback where needed to resource_id
        if label_val in values:
            # if the value is a dupe. flip it to be the VPC resource_id for both occurrences
            labeldict[node.resource_id] = node.resource_id
            for key in labeldict:
                if labeldict[key] == label_val:
                    labeldict[key] = key
        else:
            labeldict[node.resource_id] = label_val
            values.add(label_val)

    # filter to only add edges for the vpc in question
    for edge in edges:
        Graph.add_edge(
            edge.resource_config["AccepterVpcInfo"]["VpcId"],
            edge.resource_config["RequesterVpcInfo"]["VpcId"],
        )

    for n in Graph.nodes:
        if n not in labeldict:
            # theory -- there are currently peerings to VPCs that are not onboarded with Lacework
            labeldict[n] = n

    return (Graph, labeldict)


def build_target_vpc_output(
    vpc, nodes, edges, profile, output_directory, vpcs_list, vpcs_file, debug
):
    pruned_node_set = set()
    edges = [
        edge
        for edge in edges
        if edge.resource_config["AccepterVpcInfo"]["VpcId"] == vpc
        or edge.resource_config["RequesterVpcInfo"]["VpcId"] == vpc
    ]

    for edge in edges:
        pruned_node_set.add(edge.resource_config["AccepterVpcInfo"]["VpcId"])
        pruned_node_set.add(edge.resource_config["RequesterVpcInfo"]["VpcId"])

    nodes = [node for node in nodes if node.resource_id in pruned_node_set]

    if debug:
        logging.debug(f"\nParent VPC: {vpc}")
        for node in nodes:
            if node.resource_id != vpc:
                logging.debug(
                    f'\tChild: {node.resource_id} {node.resource_region} {node.account_id} {node.account_alias} {node.arn} {node.resource_config["CidrBlock"]}'
                )

    target_dir = output_directory if output_directory else f"./out/{vpc}"
    Path(target_dir).mkdir(parents=True, exist_ok=True)

    Graph, labeldict = build_graph(nodes, edges)
    pos = networkx.kamada_kawai_layout(Graph)

    if len(nodes) < 10:
        plt.figure(figsize=(10, 10), dpi=100)
        plt.margins(0.2)
    elif len(nodes) < 20:
        plt.figure(figsize=(20, 20), dpi=150)
    elif len(nodes) < 50:
        plt.figure(figsize=(25, 25), dpi=200)
    elif len(nodes) < 100:
        plt.figure(figsize=(30, 30), dpi=200)
    else:
        plt.figure(figsize=(50, 50), dpi=200)

    networkx.draw_networkx(
        Graph,
        pos,
        labels=labeldict,
        with_labels=True,
        font_size=14,
        node_color="#ADD8E6",
        edge_color="#808080",
        node_size=500,
    )
    plt.savefig(f"{target_dir}/{vpc}.png")
    plt.clf()
    Graph.clear()

    with open(f"{target_dir}/{vpc}.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=",")
        writer.writerow(
            [
                "parent_vpc",
                "connected_vpc_resource_id",
                "connected_vpc_name",
                "connected_vpc_resource_region",
                "connected_vpc_account_id",
                "connected_vpc_account_alias",
                "connected_vpc_arn",
                "connected_vpc_cidr_block",
                "vpc_peering_connection_id",
            ]
        )
        for node in nodes:
            if node.resource_id != vpc:
                vpc_peering_connection_id = [
                    edge
                    for edge in edges
                    if (
                        edge.resource_config["AccepterVpcInfo"]["VpcId"]
                        == node.resource_id
                        or edge.resource_config["RequesterVpcInfo"]["VpcId"]
                        == node.resource_id
                    )
                    and (
                        edge.resource_config["AccepterVpcInfo"]["VpcId"] == vpc
                        or edge.resource_config["RequesterVpcInfo"]["VpcId"] == vpc
                    )
                ]
                if vpc_peering_connection_id:
                    vpc_peering_connection_id = vpc_peering_connection_id[
                        0
                    ].resource_config["VpcPeeringConnectionId"]
                writer.writerow(
                    [
                        vpc,
                        node.resource_id,
                        node.tags["Name"],
                        node.resource_region,
                        node.account_id,
                        node.account_alias,
                        node.arn,
                        node.resource_config["CidrBlock"],
                        vpc_peering_connection_id,
                    ]
                )


def main(profile, output_directory, vpcs_list, vpcs_file, debug):
    try:
        lw_client = LaceworkClient(profile=profile)
    except Exception:
        raise

    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(
            format="%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
            datefmt="%Y-%m-%d:%H:%M:%S",
        )

    nodes = load_vpcs(lw_client)
    edges = load_service_connections(lw_client)

    input_vpcs = list()
    if vpcs_list:
        input_vpcs = [vpc.strip() for vpc in vpcs_list.split(",")]

    else:
        with open(vpcs_file) as f:
            for line in f:
                input_vpcs.append(line.strip())

    for vpc in input_vpcs:
        build_target_vpc_output(
            vpc,
            nodes[:],
            edges[:],
            profile,
            output_directory,
            vpcs_list,
            vpcs_file,
            debug,
        )


app = typer.Typer(no_args_is_help=True)


@app.command(no_args_is_help=True, help="Generate VPC Reports")
def vpc(
    ctx: typer.Context,
    profile: str = typer.Option(
        None,
        help="Lacework CLI profile to assume for platform interaction. 'default' is used if nothing else is specified.",
    ),
    output_directory: str = typer.Option(
        ..., help="Path to exported resutls. Default is ./out"
    ),
    vpcs_list: Optional[str] = typer.Option(
        None, help="Comma separated list of VPCs to process"
    ),
    vpcs_file: Optional[str] = typer.Option(
        None, help="Path to file containing list of VPCs to process (one VPC per line)"
    ),
    debug: bool = typer.Option(..., help="Enable debug logging"),
) -> None:

    try:
        if vpcs_file is None and vpcs_list is None:
            raise Exception("Please specify --vpcs-list or --vpcs-file")

        main(profile, output_directory, vpcs_list, vpcs_file, debug)
        print("Operation Completed Successfully!")
    except Exception as e:
        logging.error(e)


if __name__ == "__main__":
    app()
