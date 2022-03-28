from laceworkreports import common
from laceworkreports.sdk.DataHandlers import ExportHandler, QueryHandler


# cli sets configuration, sdk executes
def export() -> None:
    # connect lacework client
    common.config.connect()

    ExportHandler(
        format=common.config.format,
        results=QueryHandler(
            client=common.config.client,
            type=common.config.TYPE,
            object=common.config.OBJECT,
            filters=common.config.filters,
            returns=common.config.returns,
            lql_query=common.config.lql_query,
            dataset=common.config.dataset,
        ).execute(),
        field_map=common.config.field_map,
        file_path=common.config.file_path,
        template_path=common.config.template_path,
        dtypes=common.config.dtypes,
        db_connection=common.config.db_connection,
        db_table=common.config.db_table,
        db_if_exists=common.config.db_if_exists,
        db_create_if_missing=common.config.db_create_if_missing,
        append=common.config.append,
        flatten_json=common.config.flatten_json,
        sample=common.config.sample,
    ).export()
