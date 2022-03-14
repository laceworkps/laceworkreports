#!/bin/bash

# example: export alerts to csv
laceworkreports export alerts csv --file-path out.csv

# example: sample alerts
laceworkreports --sample export alerts csv --file-path out.csv

# example: run with lacework profile (~/.lacework.toml)
laceworkreports --profile="myprofile" --sample export alerts csv --file-path out.csv

# example: lql query for active containers
laceworkreports export queries execute csv --file-path out.csv --lql-query="@examples/lql/image.lql.txt"

# example: sample data
laceworkreports --sample export queries execute json --file-path out.csv --lql-query="@examples/lql/image.lql.txt"

# example: export host vulnerabilities with filters, returns, and field_map
laceworkreports export vulnerabilities hosts csv --file-path out.csv --filters="@examples/filters/critical_vulnerable_hosts.json" --returns="@examples/returns/hosts_short.json" --field-map="@examples/field_map/host_vulnerabilities.json"

# example: export container vulnerabilities with filters, returns, and field_map
export vulnerabilities containers csv --file-path out.csv --filters="@examples/filters/critical_vulnerable_containers.json" --returns="@examples/returns/container_short.json" --field-map="@examples/field_map/containers_vulnerabilities.json"
