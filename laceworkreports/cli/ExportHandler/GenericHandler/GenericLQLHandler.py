from types import SimpleNamespace
from typing import Optional

import json
from datetime import datetime, timedelta
from pathlib import Path

import typer

from laceworkreports import common
from laceworkreports.sdk.DataHandlers import (
    DataHandlerCliTypes,
    ExportHandler,
    QueryHandler,
)

app = typer.Typer()


@app.command()
def csv(
    ctx: typer.Context,
    start_time: datetime = typer.Option(
        (datetime.utcnow() - timedelta(days=1)).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
    ),
    end_time: datetime = typer.Option(
        (datetime.utcnow()).strftime(common.ISO_FORMAT), formats=[common.ISO_FORMAT]
    ),
    lql_query: str = typer.Option(...),
    field_map: Optional[str] = None,
    file_path: str = typer.Option(...),
    append: bool = typer.Option(common.config.append),
    flatten_json: bool = typer.Option(common.config.flatten_json),
) -> None:
    """
    Set the command context
    """

    # command context
    typer.echo(ctx.command_path)
    common.config.ACTION = str(ctx.command_path.split(" ")[-4]).replace("-", "_")
    common.config.TYPE = str(ctx.command_path.split(" ")[-3]).replace("-", "_")
    common.config.OBJECT = str(ctx.command_path.split(" ")[-2]).replace("-", "_")

    ctx.obj = SimpleNamespace()

    # handle argument defaults
    if not start_time:
        start_time = datetime.utcnow() - timedelta(days=1)

    if not end_time:
        end_time = datetime.utcnow()

    if lql_query is not None and lql_query[0] == "@":
        lql_query = Path(lql_query[1:]).read_text()

    if field_map is not None and field_map[0] == "@":
        field_map = json.loads(Path(field_map[1:]).read_text())

    # update the config namespace

    # query parameters
    common.config.start_time = start_time
    common.config.end_time = end_time
    common.config.lql_query = lql_query

    # format context
    common.config.format = DataHandlerCliTypes.CSV
    common.config.append = append
    if field_map is not None:
        common.config.field_map = field_map
    if file_path is not None:
        common.config.file_path = file_path
    if flatten_json is not None:
        common.config.flatten_json = flatten_json

    # connect lacework client
    common.config.connect()

    ExportHandler(
        format=DataHandlerCliTypes.CSV,
        results=QueryHandler(
            client=common.config.client,
            type=common.config.TYPE,
            object=common.config.OBJECT,
        ).execute(),
        field_map=common.config.field_map,
        file_path=common.config.file_path,
        append=common.config.append,
        flatten_json=common.config.flatten_json,
    ).export()


@app.command(name="json")
def json_type(
    ctx: typer.Context,
    start_time: datetime = typer.Option(
        (datetime.utcnow() - timedelta(days=1)).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
    ),
    end_time: datetime = typer.Option(
        (datetime.utcnow()).strftime(common.ISO_FORMAT), formats=[common.ISO_FORMAT]
    ),
    lql_query: str = typer.Option(...),
    field_map: Optional[str] = None,
    file_path: str = typer.Option(...),
    append: bool = typer.Option(common.config.append),
) -> None:
    """
    Set the command context
    """

    # command context
    typer.echo(ctx.command_path)
    common.config.ACTION = str(ctx.command_path.split(" ")[-4]).replace("-", "_")
    common.config.TYPE = str(ctx.command_path.split(" ")[-3]).replace("-", "_")
    common.config.OBJECT = str(ctx.command_path.split(" ")[-2]).replace("-", "_")

    ctx.obj = SimpleNamespace()

    # handle argument defaults
    if not start_time:
        start_time = datetime.utcnow() - timedelta(days=1)

    if not end_time:
        end_time = datetime.utcnow()

    if lql_query is not None and lql_query[0] == "@":
        lql_query = Path(lql_query[1:]).read_text()

    if field_map is not None and field_map[0] == "@":
        field_map = json.loads(Path(field_map[1:]).read_text())

    # update the config namespace

    # query parameters
    common.config.start_time = start_time
    common.config.end_time = end_time
    common.config.lql_query = lql_query

    # format context
    common.config.format = DataHandlerCliTypes.JSON
    common.config.append = append
    if field_map is not None:
        common.config.field_map = field_map
    if file_path is not None:
        common.config.file_path = file_path

    # connect lacework client
    common.config.connect()

    ExportHandler(
        format=DataHandlerCliTypes.JSON,
        results=QueryHandler(
            client=common.config.client,
            type=common.config.TYPE,
            object=common.config.OBJECT,
        ).execute(),
        field_map=common.config.field_map,
        file_path=common.config.file_path,
        append=common.config.append,
    ).export()


@app.command()
def postgres(
    ctx: typer.Context,
    start_time: datetime = typer.Option(
        (datetime.utcnow() - timedelta(days=1)).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
    ),
    end_time: datetime = typer.Option(
        (datetime.utcnow()).strftime(common.ISO_FORMAT), formats=[common.ISO_FORMAT]
    ),
    lql_query: str = typer.Option(...),
    field_map: Optional[str] = None,
    db_connection: str = typer.Option(...),
    db_table: str = typer.Option(common.config.db_table),
    db_if_exists: Optional[common.DBInsertTypes] = typer.Option(
        common.config.db_if_exists.value
    ),
    flatten_json: bool = typer.Option(None),
) -> None:
    """
    Set the command context
    """

    # command context
    typer.echo(ctx.command_path)
    common.config.ACTION = str(ctx.command_path.split(" ")[-4]).replace("-", "_")
    common.config.TYPE = str(ctx.command_path.split(" ")[-3]).replace("-", "_")
    common.config.OBJECT = str(ctx.command_path.split(" ")[-2]).replace("-", "_")

    ctx.obj = SimpleNamespace()

    # handle argument defaults
    if not start_time:
        start_time = datetime.utcnow() - timedelta(days=1)

    if not end_time:
        end_time = datetime.utcnow()

    if lql_query is not None and lql_query[0] == "@":
        lql_query = Path(lql_query[1:]).read_text()

    if field_map is not None and field_map[0] == "@":
        field_map = Path(field_map[1:]).read_text()

    # update the config namespace

    # query parameters
    common.config.start_time = start_time
    common.config.end_time = end_time
    common.config.lql_query = lql_query

    # format context
    common.config.format = DataHandlerCliTypes.POSTGRES
    if field_map is not None:
        common.config.field_map = field_map
    if flatten_json is not None:
        common.config.flatten_json = flatten_json

    # db context
    if db_connection is not None:
        common.config.db_connection = db_connection
    if db_table is not None:
        common.config.db_table = db_table
    if db_if_exists is not None:
        common.config.db_if_exists = db_if_exists

    # connect lacework client
    common.config.connect()

    ExportHandler(
        format=common.config.format,
        results=QueryHandler(
            client=common.config.client,
            type=common.config.TYPE,
            object=common.config.OBJECT,
        ).execute(),
        field_map=common.config.field_map,
        file_path=common.config.file_path,
        dtypes=common.config.dtypes,
        db_connection=common.config.db_connection,
        db_table=common.config.db_table,
        db_if_exists=common.config.db_if_exists,
        flatten_json=common.config.flatten_json,
    ).export()


if __name__ == "__main__":
    app()
