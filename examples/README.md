# Example

## CLI

See [CLI Example](/examples/cli)

### LQL query for active containers

```bash
laceworkreports export queries execute csv --file-path out.csv --lql-query="@examples/lql/image.lql.txt"
```

### Sample data

```bash
laceworkreports --sample export queries execute json --file-path out.csv --lql-query="@examples/lql/image.lql.txt"
```

### Export host vulnerabilities with filters, returns, and field_map

```bash
laceworkreports export vulnerabilities hosts csv --file-path out.csv --filters="@examples/filters/critical_vulnerable_hosts.json" --returns="@examples/returns/hosts_short.json" --field-map="@examples/field_map/host_vulnerabilities.json"
```

### Export container vulnerabilities with filters, returns, and field_map

```bash
laceworkreports export vulnerabilities containers csv --file-path out.csv --filters="@examples/filters/critical_vulnerable_containers.json" --returns="@examples/returns/container_short.json" --field-map="@examples/field_map/containers_vulnerabilities.json"
```

### Export jinja2 container vulnerabilities with filters, returns, and field_map

```bash
laceworkreports export vulnerabilities containers jinja --file-path out.csv --filters="@examples/filters/critical_vulnerable_containers.json" --returns="@examples/returns/container_short.json" --field-map="@examples/field_map/containers_vulnerabilities.json" --template-path="@examples/jinja2/advanced_bootstrap.html.js"
```

## SDK

See [CLI Example](/examples/sdk)

### Export DNS to CSV

```python
from laceworksdk import LaceworkClient

from laceworkreports import common
from laceworkreports.sdk.DataHandlers import (
    DataHandlerTypes,
    ExportHandler,
    QueryHandler,
)

eh = ExportHandler(
    format=DataHandlerTypes.CSV,
    results=QueryHandler(
        client=LaceworkClient(),
        type=common.ObjectTypes.Activities.value,
        object=common.ActivitiesTypes.DNSSummaries.value,
        filters=[{"field": "mid", "expression": "eq", "value": 851}],
        returns=["fqdn"],
    ).execute(),
    file_path="export.csv",
).export()
```

### Export DNS to JSON

```python
from laceworksdk import LaceworkClient

from laceworkreports import common
from laceworkreports.sdk.DataHandlers import (
    DataHandlerTypes,
    ExportHandler,
    QueryHandler,
)

eh = ExportHandler(
    format=DataHandlerTypes.JSON,
    results=QueryHandler(
        client=LaceworkClient(),
        type=common.ObjectTypes.Activities.value,
        object=common.ActivitiesTypes.DNSSummaries.value,
        filters=[{"field": "mid", "expression": "eq", "value": 851}],
        returns=["fqdn"],
    ).execute(),
    file_path="export.json",
    append=True,
).export()
```

### Export DNS to POSTGRES

```python
from laceworksdk import LaceworkClient

from laceworkreports import common
from laceworkreports.sdk.DataHandlers import (
    DataHandlerTypes,
    ExportHandler,
    QueryHandler,
)

eh = ExportHandler(
    format=DataHandlerTypes.POSTGRES,
    results=QueryHandler(
        client=LaceworkClient(),
        type=common.ObjectTypes.Activities.value,
        object=common.ActivitiesTypes.DNSSummaries.value,
        filters=[{"field": "mid", "expression": "eq", "value": 851}],
        returns=["fqdn"],
    ).execute(),
    db_table="test2",
    db_connection="postgresql://postgres:password@localhost:5433/postgres",
).export()
```
