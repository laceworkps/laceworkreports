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
from laceworkreports.sdk.ReportHelpers import ReportHelper

# report details - uses sqlite for initial sync and report due to datasize
db_table = "containers"
db_path = Path("test.db")
# db_path.unlink(missing_ok=True)
reportHelper = ReportHelper()
db_connection = f"sqlite:///{db_path.absolute()}?check_same_thread=False"

# reportHelper.sqlite_drop_table(db_table,db_connection=db_connection)
# eh = ExportHandler(
#     format=DataHandlerTypes.SQLITE,
#     results=QueryHandler(
#         client=LaceworkClient(),
#         type=common.ObjectTypes.Entities.value,
#         object=common.EntitiesTypes.Containers.value,
#     ).execute(),
#     db_table=db_table,
#     db_connection=db_connection,
# ).export()

# gcp:xxx://compute.googleapis.com/projects/xxx/zones/us-central1-a/instances/xxx
lql_query = """
                GCE {
                    source {
                        LW_CFG_GCP_ALL m
                    }
                    filter {
                        m.SERVICE = 'compute'
                        AND m.API_KEY = 'resource'
                        AND KEY_EXISTS(m.RESOURCE_CONFIG:status)
                        AND KEY_EXISTS(m.RESOURCE_CONFIG:machineType)
                    }
                    return distinct { 
                        'fubotv' AS lwAccount,
                        'gcp:' || ORGANIZATION::String || ':' || SUBSTRING(
                            SUBSTRING(
                                m.URN,
                                CHAR_INDEX(
                                    '/', 
                                    m.URN
                                )+34,
                                LENGTH(m.URN)
                            ),
                            0,
                            CHAR_INDEX(
                                '/zones/',
                                SUBSTRING(
                                    m.URN,
                                    CHAR_INDEX(
                                        '/', 
                                        m.URN
                                    )+35,
                                    LENGTH(m.URN)
                                )
                            )
                        ) AS accountId,
                        m.RESOURCE_CONFIG:id::string AS instanceId,
                        m.RESOURCE_CONFIG:status::String AS status
                    }
                }
                """

reportHelper.sqlite_drop_table("machines", db_connection=db_connection)
eh = ExportHandler(
    format=DataHandlerTypes.SQLITE,
    results=QueryHandler(
        client=LaceworkClient(),
        type=common.ObjectTypes.Queries.value,
        object=common.QueriesTypes.Execute.value,
        lql_query=lql_query,
    ).execute(),
    db_table="machines",
    db_connection=db_connection,
).export()
