"""
Example usage of laceworkreports sdk
"""

from datetime import datetime, timedelta
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
db_table = "machines"
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
# gcp_query =     """
#                 GCE {
#                     source {
#                         LW_CFG_GCP_ALL m
#                     }
#                     filter {
#                         m.SERVICE = 'compute'
#                         AND m.API_KEY = 'resource'
#                         AND KEY_EXISTS(m.RESOURCE_CONFIG:status)
#                         AND KEY_EXISTS(m.RESOURCE_CONFIG:machineType)
#                     }
#                     return distinct {
#                         'fubotv' AS lwAccount,
#                         'gcp:' || ORGANIZATION::String || ':' || SUBSTRING(
#                             SUBSTRING(
#                                 m.URN,
#                                 CHAR_INDEX(
#                                     '/',
#                                     m.URN
#                                 )+34,
#                                 LENGTH(m.URN)
#                             ),
#                             0,
#                             CHAR_INDEX(
#                                 '/zones/',
#                                 SUBSTRING(
#                                     m.URN,
#                                     CHAR_INDEX(
#                                         '/',
#                                         m.URN
#                                     )+35,
#                                     LENGTH(m.URN)
#                                 )
#                             )
#                         ) AS accountId,
#                         m.RESOURCE_CONFIG:id::string AS instanceId,
#                         m.RESOURCE_CONFIG:status::String AS status,
#                         m.RESOURCE_CONFIG:labels AS labels,
#                         m.RESOURCE_CONFIG
#                     }
#                 }
#                 """
# aws_query =    f"""
#                 ECS {{
#                     source {{LW_CFG_AWS_EC2_INSTANCES m}}
#                     return distinct {{
#                             'aws:' || m.ACCOUNT_ID AS accountId,
#                             m.RESOURCE_ID AS instanceId,
#                             CHAR_INDEX(
#                                         '"Name",',
#                                         m.RESOURCE_CONFIG:Tags::string
#                                     ) as Test,
#                             SUBSTRING(
#                                 SUBSTRING(
#                                     m.RESOURCE_CONFIG:Tags::string,
#                                     CHAR_INDEX(
#                                         '"Name",',
#                                         m.RESOURCE_CONFIG:Tags::string
#                                     )+16,
#                                     LENGTH(m.RESOURCE_CONFIG:Tags::string)
#                                 ),
#                                 0,
#                                 CHAR_INDEX(
#                                     '"',
#                                     SUBSTRING(
#                                         m.RESOURCE_CONFIG:Tags::string,
#                                         CHAR_INDEX(
#                                             '"Name",',
#                                             m.RESOURCE_CONFIG:Tags::string
#                                         )+17,
#                                         LENGTH(m.RESOURCE_CONFIG:Tags::string)
#                                     )
#                                 )
#                             ) AS name,
#                             m.RESOURCE_CONFIG:State.Name::String AS state,
#                             m.RESOURCE_CONFIG:Tags AS tags
#                         }}
#                     }}
#                 """

# reportHelper.sqlite_drop_table("gcp_machines", db_connection=db_connection)
# eh = ExportHandler(
#     format=DataHandlerTypes.SQLITE,
#     results=QueryHandler(
#         client=LaceworkClient(),
#         type=common.ObjectTypes.Queries.value,
#         object=common.QueriesTypes.Execute.value,
#         lql_query=gcp_query,
#     ).execute(),
#     db_table="gcp_machines",
#     db_connection=db_connection,
# ).export()

# reportHelper.sqlite_drop_table("aws_machines", db_connection=db_connection)
# eh = ExportHandler(
#     format=DataHandlerTypes.SQLITE,
#     results=QueryHandler(
#         client=LaceworkClient(),
#         type=common.ObjectTypes.Queries.value,
#         object=common.QueriesTypes.Execute.value,
#         lql_query=aws_query,
#     ).execute(),
#     db_table="aws_machines",
#     db_connection=db_connection,
# ).export()

# lql_query =     """
#                 RDS {
#                     source {
#                         LW_CFG_AWS_RDS_DB_INSTANCES
#                     }
#                     return {
#                         LW_CFG_AWS_RDS_DB_INSTANCES.*
#                     }
#                 }
#                 """

# reportHelper.sqlite_drop_table(db_table=db_table, db_connection=db_connection)
# eh = ExportHandler(
#     format=DataHandlerTypes.SQLITE,
#     results=QueryHandler(
#         client=LaceworkClient(),
#         type=common.ObjectTypes.Queries.value,
#         object=common.QueriesTypes.Execute.value,
#         lql_query=lql_query,
#     ).execute(),
#     db_table=db_table,
#     db_connection=db_connection,
# ).export()

lql_query = """
                RDS {
                    source {
                        LW_HE_CONTAINERS
                    }
                    return distinct {
                        LW_HE_CONTAINERS.*
                    }
                }
                """

reportHelper.sqlite_drop_table(db_table="containers", db_connection=db_connection)
eh = ExportHandler(
    format=DataHandlerTypes.SQLITE,
    results=QueryHandler(
        start_time=datetime.utcnow() - timedelta(days=2),
        end_time=datetime.utcnow(),
        client=LaceworkClient(),
        type=common.ObjectTypes.Queries.value,
        object=common.QueriesTypes.Execute.value,
        lql_query=lql_query,
    ).execute(),
    db_table="containers",
    db_connection=db_connection,
).export()

# lql_query =     """
#                 RDS {
#                     source {
#                         LW_HE_MACHINES
#                     }
#                     return {
#                         LW_HE_MACHINES.*
#                     }
#                 }
#                 """

# reportHelper.sqlite_drop_table(db_table="machines", db_connection=db_connection)
# eh = ExportHandler(
#     format=DataHandlerTypes.SQLITE,
#     results=QueryHandler(
#         start_time=datetime.utcnow() - timedelta(days=2),
#         end_time=datetime.utcnow(),
#         client=LaceworkClient(),
#         type=common.ObjectTypes.Queries.value,
#         object=common.QueriesTypes.Execute.value,
#         lql_query=lql_query,
#     ).execute(),
#     db_table="machines",
#     db_connection=db_connection,
# ).export()

# reportHelper.sqlite_drop_table("test2", db_connection=db_connection)
# reportHelper.sqlite_execute(
#     "create table test2 as select 'three' as test;", db_connection=db_connection
# )

# test = reportHelper.sqlite_queries(
#     {"test": "select * from test2"}, db_table="test", db_connection=db_connection
# )
# print(test)
