import typer

from laceworkreports import common
from laceworkreports.cli.ExportHandlers.DataExportHandlers import ConfigsAPIv2Handler

app = typer.Typer()

parent_command = common.ActionTypes.Export.value
self_command = common.ObjectTypes[__name__.split(".")[-1]].value

commands = []

for t in common.ConfigsTypes:
    commands.append({"command_name": t.value, "command_type": ConfigsAPIv2Handler.app})

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
