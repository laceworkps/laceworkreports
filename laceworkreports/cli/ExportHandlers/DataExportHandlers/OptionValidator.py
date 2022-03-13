import json
from datetime import datetime
from pathlib import Path

import typer

from laceworkreports import common


def validate(
    start_time=None,
    end_time=None,
    returns=None,
    filters=None,
    field_map=None,
    file_path=None,
    lql_query=None,
    flatten_json=None,
    template_path=None,
    dataset=None,
    db_connection=None,
    db_table=None,
    db_if_exists=None,
    append=None,
) -> dict:
    # query filters
    if start_time is not None and end_time is not None:
        if not isinstance(start_time, datetime):
            raise typer.BadParameter("Invalid start date")

        if not isinstance(end_time, datetime):
            raise typer.BadParameter("Invalid start date")

        if start_time > end_time:
            raise typer.BadParameter("Start time cannot be greater than end time")

    if returns is not None:
        if returns[0] == "@":
            if not Path.exists(returns[1:]):
                raise typer.BadParameter("Returns path does not exist")
            try:
                returns = json.loads(Path(returns[1:]).read_text())
            except Exception as e:
                raise typer.BadParameter(f"Failed to parse returns json: {e}")
        else:
            try:
                returns = json.loads(returns)
            except Exception as e:
                raise typer.BadParameter(f"Failed to parse returns json: {e}")

    if filters is not None:
        if filters[0] == "@":
            if not Path.exists(filters[1:]):
                raise typer.BadParameter("Filters path does not exist")
            try:
                filters = json.loads(Path(filters[1:]).read_text())
            except Exception as e:
                raise typer.BadParameter(f"Failed to parse filters json: {e}")
        else:
            try:
                filters = json.loads(filters)
            except Exception as e:
                raise typer.BadParameter(f"Failed to parse filters json: {e}")

    if field_map is not None:
        if field_map[0] == "@":
            if not Path.exists(field_map[1:]):
                raise typer.BadParameter("Field map path does not exist")
            try:
                field_map = json.loads(Path(field_map[1:]).read_text())
            except Exception as e:
                raise typer.BadParameter(f"Failed to parse field map json: {e}")
        else:
            try:
                field_map = json.loads(field_map)
            except Exception as e:
                raise typer.BadParameter(f"Failed to parse field map json: {e}")

    # lql
    if lql_query is not None:
        pass

    # jinja
    if template_path is not None:
        if not Path.exists(template_path):
            raise typer.BadParameter("Template path does not exist")

    # postgres
    if db_connection is not None:
        pass
    if db_table is not None:
        pass
    if db_if_exists is not None:
        pass

    return {
        "start_time": start_time,
        "end_time": end_time,
        "returns": returns,
        "filters": filters,
        "field_map": field_map,
        "file_path": file_path,
        "lql_query": lql_query,
        "flatten_json": flatten_json,
        "template_path": template_path,
        "dataset": dataset,
        "db_connection": db_connection,
        "db_table": db_table,
        "db_if_exists": db_if_exists,
        "append": append,
    }


def update_config(options) -> bool:
    for k in options.keys():
        if options[k] is not None:
            common.config.update(k, options[k])
