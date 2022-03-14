from typing import Any

import json
from datetime import datetime
from pathlib import Path

import typer

from laceworkreports import common


def validate(
    start_time: Any = None,
    end_time: Any = None,
    returns: Any = None,
    filters: Any = None,
    field_map: Any = None,
    file_path: Any = None,
    lql_query: Any = None,
    flatten_json: Any = None,
    template_path: Any = None,
    dataset: Any = None,
    db_connection: Any = None,
    db_table: Any = None,
    db_if_exists: Any = None,
    db_create_if_missing: Any = None,
    append: Any = None,
) -> Any:
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
            if not Path(returns[1:]).exists():
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
            if not Path(filters[1:]).exists():
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
            if not Path(field_map[1:]).exists():
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
        if lql_query[0] == "@":
            if not Path(lql_query[1:]).exists():
                raise typer.BadParameter("LQL query path does not exist")
            try:
                lql_query = Path(lql_query[1:]).read_text()
            except Exception as e:
                raise typer.BadParameter(f"Failed to parse lql query: {e}")

    # jinja
    if template_path is not None:
        if not Path(template_path).exists():
            raise typer.BadParameter("Template path does not exist")

    # postgres - could add connection validation here
    if db_connection is not None:
        pass
    if db_table is not None:
        pass
    if db_if_exists is not None:
        pass
    if db_create_if_missing is not None:
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
        "db_create_if_missing": db_create_if_missing,
        "append": append,
    }


def update_config(options: Any) -> bool:
    if options["start_time"] is not None:
        common.config.start_time = options["start_time"]
    if options["end_time"] is not None:
        common.config.end_time = options["end_time"]
    if options["returns"] is not None:
        common.config.returns = options["returns"]
    if options["filters"] is not None:
        common.config.filters = options["filters"]
    if options["field_map"] is not None:
        common.config.field_map = options["field_map"]
    if options["file_path"] is not None:
        common.config.file_path = options["file_path"]
    if options["lql_query"] is not None:
        common.config.lql_query = options["lql_query"]
    if options["flatten_json"] is not None:
        common.config.flatten_json = options["flatten_json"]
    if options["template_path"] is not None:
        common.config.template_path = options["template_path"]
    if options["dataset"] is not None:
        common.config.dataset = options["dataset"]
    if options["db_connection"] is not None:
        common.config.db_connection = options["db_connection"]
    if options["db_table"] is not None:
        common.config.db_table = options["db_table"]
    if options["db_if_exists"] is not None:
        common.config.db_if_exists = options["db_if_exists"]
    if options["db_create_if_missing"] is not None:
        common.config.db_create_if_missing = options["db_create_if_missing"]
    if options["append"] is not None:
        common.config.append = options["append"]

    return True
