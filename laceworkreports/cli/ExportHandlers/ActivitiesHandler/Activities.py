"""
Activities
"""
import typer

from laceworkreports import common
from laceworkreports.cli.ExportHandlers.DataExportHandlers import GenericAPIv2Handler

app = typer.Typer(no_args_is_help=True)

parent_command = common.ActionTypes.Export.value
self_command = common.ObjectTypes[__name__.split(".")[-1]].value

commands = []

for t in common.ActivitiesTypes:
    commands.append({"command_name": t.value, "command_type": GenericAPIv2Handler.app})

for command in iter(commands):
    app.add_typer(
        command["command_type"],
        name=command["command_name"],
        help=f"Retrieve lacework activities api {command['command_name']} events",
        no_args_is_help=True,
        epilog=f"{common.config.name} {parent_command} {self_command} {command['command_name']} <exporttype> [OPTIONS]",
    )

if __name__ == "__main__":
    app()
