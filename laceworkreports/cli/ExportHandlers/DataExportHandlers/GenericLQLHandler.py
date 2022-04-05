"""
LQL Handler
"""

from typing import Optional

from datetime import datetime, timedelta

import typer

from laceworkreports import common
from laceworkreports.sdk.DataHandlers import DataHandlerCliTypes

from .GenericExport import export
from .OptionValidator import update_config, validate

app = typer.Typer()


@app.command(no_args_is_help=True, help="Export to csv")
def csv(
    ctx: typer.Context,
    start_time: datetime = typer.Option(
        (datetime.utcnow() - timedelta(hours=25)).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
        help="Start time for query period",
    ),
    end_time: datetime = typer.Option(
        (datetime.utcnow()).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
        help="End time for query period",
    ),
    lql_query: str = typer.Option(
        ..., help="LQL query string for file path (use @ to specify file path)"
    ),
    field_map: Optional[str] = typer.Option(
        None, help="LQL query string for file path (use @ to specify file path)"
    ),
    file_path: str = typer.Option(
        ...,
        help="Path to exported CSV result",
        envvar=common.LACEWORK_REPORTS_FILE_PATH,
    ),
    append: bool = typer.Option(
        common.config.append, help="Boolean value to append or replace results"
    ),
    flatten_json: bool = typer.Option(
        common.config.flatten_json, help="Boolean value to flatten json result or not"
    ),
) -> None:
    """
    Set the command context
    """

    # command context
    common.config.ACTION = str(ctx.command_path.split(" ")[-4]).replace("-", "_")
    common.config.TYPE = str(ctx.command_path.split(" ")[-3]).replace("-", "_")
    common.config.OBJECT = str(ctx.command_path.split(" ")[-2]).replace("-", "_")
    common.config.format = DataHandlerCliTypes.CSV

    options = validate(
        start_time=start_time,
        end_time=end_time,
        field_map=field_map,
        file_path=file_path,
        lql_query=lql_query,
        flatten_json=flatten_json,
        append=append,
    )

    update_config(options=options)

    # after setting context use sdk to execute
    export()


@app.command(name="json", no_args_is_help=True, help="Export to json")
def json_type(
    ctx: typer.Context,
    start_time: datetime = typer.Option(
        (datetime.utcnow() - timedelta(hours=25)).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
        help="Start time for query period",
    ),
    end_time: datetime = typer.Option(
        (datetime.utcnow()).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
        help="End time for query period",
    ),
    lql_query: str = typer.Option(
        ..., help="LQL query string for file path (use @ to specify file path)"
    ),
    field_map: Optional[str] = typer.Option(
        None, help="LQL query string for file path (use @ to specify file path)"
    ),
    file_path: str = typer.Option(
        ...,
        help="Path to exported JSON result",
        envvar=common.LACEWORK_REPORTS_FILE_PATH,
    ),
    append: bool = typer.Option(
        common.config.append, help="Boolean value to append or replace results"
    ),
) -> None:
    """
    Set the command context
    """

    # command context
    common.config.ACTION = str(ctx.command_path.split(" ")[-4]).replace("-", "_")
    common.config.TYPE = str(ctx.command_path.split(" ")[-3]).replace("-", "_")
    common.config.OBJECT = str(ctx.command_path.split(" ")[-2]).replace("-", "_")
    common.config.format = DataHandlerCliTypes.JSON

    options = validate(
        start_time=start_time,
        end_time=end_time,
        field_map=field_map,
        file_path=file_path,
        lql_query=lql_query,
        append=append,
    )

    update_config(options=options)

    # after setting context use sdk to execute
    export()


@app.command(no_args_is_help=True, help="Export to postgres database")
def postgres(
    ctx: typer.Context,
    start_time: datetime = typer.Option(
        (datetime.utcnow() - timedelta(hours=25)).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
        help="Start time for query period",
    ),
    end_time: datetime = typer.Option(
        (datetime.utcnow()).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
        help="End time for query period",
    ),
    lql_query: str = typer.Option(
        ..., help="LQL query string for file path (use @ to specify file path)"
    ),
    field_map: Optional[str] = typer.Option(
        None,
        help="JSON fieldmap to alias results columns. For file path (use @ to specify file path)",
    ),
    flatten_json: bool = typer.Option(
        common.config.flatten_json, help="Boolean value to flatten json result or not"
    ),
    db_connection: str = typer.Option(
        ...,
        help="Postgres connection string (e.g. postgresql://postgres:password@localhost:5432/postgres)",
        envvar=common.LACEWORK_REPORTS_DB_CONNECTION,
    ),
    db_table: str = typer.Option(
        common.config.db_table,
        help="Postgres table to store results",
        envvar=common.LACEWORK_REPORTS_DB_TABLE,
    ),
    db_if_exists: Optional[common.DBInsertTypes] = typer.Option(
        common.config.db_if_exists.value,
        help="Action to take if db table already exists",
    ),
    db_create_if_missing: Optional[bool] = typer.Option(
        common.config.db_create_if_missing,
        help="Bool to create database if missing",
    ),
) -> None:
    """
    Set the command context
    """

    # command context
    common.config.ACTION = str(ctx.command_path.split(" ")[-4]).replace("-", "_")
    common.config.TYPE = str(ctx.command_path.split(" ")[-3]).replace("-", "_")
    common.config.OBJECT = str(ctx.command_path.split(" ")[-2]).replace("-", "_")
    common.config.format = DataHandlerCliTypes.POSTGRES

    options = validate(
        start_time=start_time,
        end_time=end_time,
        field_map=field_map,
        lql_query=lql_query,
        flatten_json=flatten_json,
        db_connection=db_connection,
        db_table=db_table,
        db_if_exists=db_if_exists,
        db_create_if_missing=db_create_if_missing,
    )

    update_config(options=options)

    # after setting context use sdk to execute
    export()


@app.command(
    no_args_is_help=True, help="Use jinja template to transform export results"
)
def jinja2(
    ctx: typer.Context,
    start_time: datetime = typer.Option(
        (datetime.utcnow() - timedelta(hours=25)).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
        help="Start time for query period",
    ),
    end_time: datetime = typer.Option(
        (datetime.utcnow()).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
        help="End time for query period",
    ),
    lql_query: str = typer.Option(
        ..., help="LQL query string for file path (use @ to specify file path)"
    ),
    field_map: Optional[str] = typer.Option(
        None,
        help="JSON fieldmap to alias results columns. For file path (use @ to specify file path)",
    ),
    file_path: str = typer.Option(
        ...,
        help="Path to exported JINJA2 result",
        envvar=common.LACEWORK_REPORTS_FILE_PATH,
    ),
    flatten_json: bool = typer.Option(
        common.config.flatten_json, help="Boolean value to flatten json result or not"
    ),
    template_path: str = typer.Option(
        ...,
        help="Path to jinja2 template. Results will be passed as 'dataset' variable.",
        envvar=common.LACEWORK_REPORTS_TEMPLATE_PATH,
    ),
) -> None:
    """
    Set the command context
    """

    # command context
    common.config.ACTION = str(ctx.command_path.split(" ")[-4]).replace("-", "_")
    common.config.TYPE = str(ctx.command_path.split(" ")[-3]).replace("-", "_")
    common.config.OBJECT = str(ctx.command_path.split(" ")[-2]).replace("-", "_")
    common.config.format = DataHandlerCliTypes.JINJA2

    options = validate(
        start_time=start_time,
        end_time=end_time,
        field_map=field_map,
        file_path=file_path,
        lql_query=lql_query,
        flatten_json=flatten_json,
        template_path=template_path,
    )

    update_config(options=options)

    # after setting context use sdk to execute
    export()


if __name__ == "__main__":
    app()
