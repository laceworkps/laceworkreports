"""
APIv2 Handler
"""

from typing import Optional

import json
from datetime import datetime, timedelta
from pathlib import Path

import typer

from laceworkreports import common
from laceworkreports.sdk.DataHandlers import DataHandlerCliTypes

from .GenericExport import export

app = typer.Typer(no_args_is_help=True)


@app.command(no_args_is_help=True, help="Export to csv")
def csv(
    ctx: typer.Context,
    start_time: datetime = typer.Option(
        (datetime.utcnow() - timedelta(days=1)).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
    ),
    end_time: datetime = typer.Option(
        (datetime.utcnow()).strftime(common.ISO_FORMAT), formats=[common.ISO_FORMAT]
    ),
    returns: Optional[str] = None,
    filters: Optional[str] = None,
    dataset: common.ComplianceEvaluationsTypes = typer.Option(
        common.ComplianceEvaluationsTypes.AwsCompliance.value
    ),
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

    # handle argument defaults
    if not start_time:
        start_time = datetime.utcnow() - timedelta(days=1)

    if not end_time:
        end_time = datetime.utcnow()

    if returns is not None and returns[0] == "@":
        returns = json.loads(Path(returns[1:]).read_text())

    if filters is not None and filters[0] == "@":
        returns = json.loads(Path(filters[1:]).read_text())

    if field_map is not None and field_map[0] == "@":
        field_map = json.loads(Path(field_map[1:]).read_text())

    # update the config namespace

    # query parameters
    common.config.start_time = start_time
    common.config.end_time = end_time
    common.config.dataset = dataset
    if filters is not None:
        common.config.filters = filters
    if returns is not None:
        common.config.returns = returns

    # format context
    common.config.format = DataHandlerCliTypes.CSV
    common.config.append = append
    if field_map is not None:
        common.config.field_map = field_map
    if file_path is not None:
        common.config.file_path = file_path
    if flatten_json is not None:
        common.config.flatten_json = flatten_json

    # after setting context use sdk to execute
    export()


@app.command(name="json", no_args_is_help=True, help="Export to json")
def json_type(
    ctx: typer.Context,
    start_time: datetime = typer.Option(
        (datetime.utcnow() - timedelta(days=1)).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
    ),
    end_time: datetime = typer.Option(
        (datetime.utcnow()).strftime(common.ISO_FORMAT), formats=[common.ISO_FORMAT]
    ),
    returns: Optional[str] = None,
    filters: Optional[str] = None,
    dataset: common.ComplianceEvaluationsTypes = typer.Option(
        common.ComplianceEvaluationsTypes.AwsCompliance.value
    ),
    field_map: Optional[str] = None,
    append: bool = typer.Option(common.config.append),
    file_path: str = typer.Option(...),
) -> None:
    """
    Set the command context
    """

    # command context
    typer.echo(ctx.command_path)
    common.config.ACTION = str(ctx.command_path.split(" ")[-4]).replace("-", "_")
    common.config.TYPE = str(ctx.command_path.split(" ")[-3]).replace("-", "_")
    common.config.OBJECT = str(ctx.command_path.split(" ")[-2]).replace("-", "_")

    # handle argument defaults
    if not start_time:
        start_time = datetime.utcnow() - timedelta(days=1)

    if not end_time:
        end_time = datetime.utcnow()

    if returns is not None and returns[0] == "@":
        returns = json.loads(Path(returns[1:]).read_text())

    if filters is not None and filters[0] == "@":
        returns = json.loads(Path(filters[1:]).read_text())

    if field_map is not None and field_map[0] == "@":
        field_map = json.loads(Path(field_map[1:]).read_text())

    # update the config namespace

    # query parameters
    common.config.start_time = start_time
    common.config.end_time = end_time
    if filters is not None:
        common.config.filters = filters
    if returns is not None:
        common.config.returns = returns

    # format context
    common.config.format = DataHandlerCliTypes.JSON
    common.config.append = append
    common.config.dataset = dataset
    if field_map is not None:
        common.config.field_map = field_map
    if file_path is not None:
        common.config.file_path = file_path

    # after setting context use sdk to execute
    export()


@app.command(no_args_is_help=True, help="Export to postgres database")
def postgres(
    ctx: typer.Context,
    start_time: datetime = typer.Option(
        (datetime.utcnow() - timedelta(days=1)).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
    ),
    end_time: datetime = typer.Option(
        (datetime.utcnow()).strftime(common.ISO_FORMAT), formats=[common.ISO_FORMAT]
    ),
    returns: Optional[str] = None,
    filters: Optional[str] = None,
    dataset: common.ComplianceEvaluationsTypes = typer.Option(
        common.ComplianceEvaluationsTypes.AwsCompliance.value
    ),
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

    # handle argument defaults
    if not start_time:
        start_time = datetime.utcnow() - timedelta(days=1)

    if not end_time:
        end_time = datetime.utcnow()

    if returns is not None and returns[0] == "@":
        returns = json.loads(Path(returns[1:]).read_text())

    if filters is not None and filters[0] == "@":
        returns = json.loads(Path(filters[1:]).read_text())

    if field_map is not None and field_map[0] == "@":
        field_map = json.loads(Path(field_map[1:]).read_text())

    # update the config namespace

    # query parameters
    common.config.start_time = start_time
    common.config.end_time = end_time
    common.config.dataset = dataset
    if filters is not None:
        common.config.filters = filters
    if returns is not None:
        common.config.returns = returns

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

    # after setting context use sdk to execute
    export()


@app.command(
    no_args_is_help=True, help="Use jinja template to transform export results"
)
def jinja2(
    ctx: typer.Context,
    start_time: datetime = typer.Option(
        (datetime.utcnow() - timedelta(days=1)).strftime(common.ISO_FORMAT),
        formats=[common.ISO_FORMAT],
    ),
    end_time: datetime = typer.Option(
        (datetime.utcnow()).strftime(common.ISO_FORMAT), formats=[common.ISO_FORMAT]
    ),
    returns: Optional[str] = None,
    filters: Optional[str] = None,
    dataset: common.ComplianceEvaluationsTypes = typer.Option(
        common.ComplianceEvaluationsTypes.AwsCompliance.value
    ),
    field_map: Optional[str] = None,
    file_path: str = typer.Option(...),
    template_path: str = typer.Option(...),
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

    # handle argument defaults
    if not start_time:
        start_time = datetime.utcnow() - timedelta(days=1)

    if not end_time:
        end_time = datetime.utcnow()

    if returns is not None and returns[0] == "@":
        returns = json.loads(Path(returns[1:]).read_text())

    if filters is not None and filters[0] == "@":
        returns = json.loads(Path(filters[1:]).read_text())

    if field_map is not None and field_map[0] == "@":
        field_map = json.loads(Path(field_map[1:]).read_text())

    # update the config namespace

    # query parameters
    common.config.start_time = start_time
    common.config.end_time = end_time
    common.config.dataset = dataset
    if filters is not None:
        common.config.filters = filters
    if returns is not None:
        common.config.returns = returns

    # format context
    common.config.format = DataHandlerCliTypes.JINJA2
    common.config.append = append
    if field_map is not None:
        common.config.field_map = field_map
    if template_path is not None:
        common.config.template_path = template_path
    if file_path is not None:
        common.config.file_path = file_path
    if flatten_json is not None:
        common.config.flatten_json = flatten_json

    # after setting context use sdk to execute
    export()


if __name__ == "__main__":
    app()
