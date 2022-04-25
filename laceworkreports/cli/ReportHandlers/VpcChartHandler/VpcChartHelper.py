import csv
import logging

import matplotlib.pyplot as plt
import networkx


def build_graph(nodes, edges):
    Graph = networkx.Graph()
    labeldict = {}

    values = set()
    for node in nodes:
        Graph.add_node(node["RESOURCE_ID"])
        label_val = (
            node["RESOURCE_TAGS"]["Name"]
            if "Name" in node["RESOURCE_TAGS"] and node["RESOURCE_TAGS"]["Name"] != ""
            else node["RESOURCE_ID"]
        )
        # handle cases with duplicate labels, fallback where needed to resource_id
        if label_val in values:
            # if the value is a dupe. flip it to be the VPC resource_id for both occurrences
            labeldict[node["RESOURCE_ID"]] = node["RESOURCE_ID"]
            for key in labeldict:
                if labeldict[key] == label_val:
                    labeldict[key] = key
        else:
            labeldict[node["RESOURCE_ID"]] = label_val
            values.add(label_val)

    # filter to only add edges for the vpc in question
    for edge in edges:
        Graph.add_edge(
            edge["RESOURCE_CONFIG"]["AccepterVpcInfo"]["VpcId"],
            edge["RESOURCE_CONFIG"]["RequesterVpcInfo"]["VpcId"],
        )

    for n in Graph.nodes:
        if n not in labeldict:
            # theory -- there are currently peerings to VPCs that are not onboarded with Lacework
            labeldict[n] = n

    return (Graph, labeldict)


def build_target_vpc_output(vpc, nodes, edges, output_directory):
    pruned_node_set = set()
    edges = [
        edge
        for edge in edges
        if (
            edge.get("RESOURCE_CONFIG") is not None
            and edge["RESOURCE_CONFIG"]["AccepterVpcInfo"]["VpcId"] == vpc
        )
        or (
            edge.get("RESOURCE_CONFIG") is not None
            and edge["RESOURCE_CONFIG"]["RequesterVpcInfo"]["VpcId"] == vpc
        )
    ]

    for edge in edges:
        pruned_node_set.add(edge["RESOURCE_CONFIG"]["AccepterVpcInfo"]["VpcId"])
        pruned_node_set.add(edge["RESOURCE_CONFIG"]["RequesterVpcInfo"]["VpcId"])

    nodes = [node for node in nodes if node["RESOURCE_ID"] in pruned_node_set]

    if logging.root.level == logging.DEBUG:
        logging.debug(f"\nParent VPC: {vpc}")
        for node in nodes:
            if node["RESOURCE_ID"] != vpc:
                logging.debug(
                    f"\tChild: {node['RESOURCE_ID']} {node['RESOURCE_REGION']} {node['ACCOUNT_ID']} {node['ACCOUNT_ALIAS']} {node['ARN']} {node['RESOURCE_CONFIG']['CidrBlock']}"
                )

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
    plt.savefig(f"{output_directory}/{vpc}.png")
    plt.clf()
    Graph.clear()

    with open(f"{output_directory}/{vpc}.csv", "w", newline="") as csvfile:
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
            if node["RESOURCE_ID"] != vpc:
                vpc_peering_connection_id = [
                    edge
                    for edge in edges
                    if (
                        edge["RESOURCE_CONFIG"]["AccepterVpcInfo"]["VpcId"]
                        == node["RESOURCE_ID"]
                        or edge["RESOURCE_CONFIG"]["RequesterVpcInfo"]["VpcId"]
                        == node["RESOURCE_ID"]
                    )
                    and (
                        edge["RESOURCE_CONFIG"]["AccepterVpcInfo"]["VpcId"] == vpc
                        or edge["RESOURCE_CONFIG"]["RequesterVpcInfo"]["VpcId"] == vpc
                    )
                ]
                if vpc_peering_connection_id:
                    vpc_peering_connection_id = vpc_peering_connection_id[0][
                        "RESOURCE_CONFIG"
                    ]["VpcPeeringConnectionId"]

                writer.writerow(
                    [
                        vpc,
                        node["RESOURCE_ID"],
                        node["RESOURCE_TAGS"].get("Name"),
                        node["RESOURCE_REGION"],
                        node["ACCOUNT_ID"],
                        node["ACCOUNT_ALIAS"],
                        node["ARN"],
                        node["RESOURCE_CONFIG"]["CidrBlock"],
                        vpc_peering_connection_id,
                    ]
                )
