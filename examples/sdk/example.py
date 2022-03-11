"""
Example usage of laceworkreports sdk
"""

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
