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

dns_entries = ExportHandler(
    format=DataHandlerTypes.DICT,
    results=QueryHandler(
        client=LaceworkClient(),
        type=common.ObjectTypes.Activities.value,
        object=common.ActivitiesTypes.DNSSummaries.value,
        filters=[{"field": "mid", "expression": "eq", "value": 851}],
        returns=["fqdn"],
    ).execute(),
).export()

print(dns_entries)
