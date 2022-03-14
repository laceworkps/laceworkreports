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

lql_query = """
Custom_HE_Container_1 {
    source {
        LW_HE_CONTAINERS
    }
    return distinct {
        IMAGE_ID
    }
}
"""

active_images = ExportHandler(
    format=DataHandlerTypes.DICT,
    results=QueryHandler(
        client=LaceworkClient(),
        type=common.ObjectTypes.Queries.value,
        object=common.QueriesTypes.Execute.value,
        lql_query=lql_query,
    ).execute(),
    field_map={"imageId": "IMAGE_ID"},
).export()


image_ids = list({d["imageId"] for d in active_images if "imageId" in d.keys()})

active_vulnerable_containers = ExportHandler(
    format=DataHandlerTypes.CSV,
    results=QueryHandler(
        client=LaceworkClient(),
        type=common.ObjectTypes.Vulnerabilities.value,
        object=common.VulnerabilitiesTypes.Containers.value,
        filters=[
            {
                "field": "severity",
                "expression": "in",
                "values": ["Critical", "High", "Medium"],
            },
            {"field": "imageId", "expression": "in", "values": image_ids},
            {"field": "status", "expression": "in", "values": ["VULNERABLE"]},
            {"field": "fixInfo.fix_available", "expression": "eq", "value": 1},
        ],
        returns=[
            "startTime",
            "imageId",
            "severity",
            "status",
            "vulnId",
            "evalCtx",
            "fixInfo",
            "featureKey",
        ],
    ).execute(),
    field_map={
        "start_time": "startTime",
        "image_id": "imageId",
        "cve_id": "vulnId",
        "image_registry": "evalCtx.image_info.registry",
        "image_repo": "evalCtx.image_info.repo",
        "image_status": "evalCtx.image_info.status",
        "package_name": "featureKey.name",
        "package_namespace": "featureKey.namespace",
        "version": "featureKey.version",
        "fix_available": "fixInfo.fix_available",
        "fixed_version": "fixInfo.fixed_version",
        "severity": "severity",
        "status": "status",
    },
    file_path="export.csv",
).export()
