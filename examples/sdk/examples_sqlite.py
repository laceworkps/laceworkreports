"""
Example usage of laceworkreports sdk
"""

from pathlib import Path

from laceworksdk import LaceworkClient

from laceworkreports import common
from laceworkreports.sdk.DataHandlers import (
    DataHandlerTypes,
    ExportHandler,
    QueryHandler,
)

# report details - uses sqlite for initial sync and report due to datasize
db_table = "containers"
db_path = Path("containers.db")
# db_path.unlink(missing_ok=True)
db_connection = f"sqlite:///{db_path.absolute()}?check_same_thread=False"

eh = ExportHandler(
    format=DataHandlerTypes.SQLITE,
    results=QueryHandler(
        client=LaceworkClient(),
        type=common.ObjectTypes.Entities.value,
        object=common.EntitiesTypes.Containers.value,
    ).execute(),
    db_table=db_table,
    db_connection=db_connection,
).export()
