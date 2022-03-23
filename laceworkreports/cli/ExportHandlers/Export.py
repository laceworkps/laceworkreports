import typer

from laceworkreports import common

from .ActivitiesHandler import Activities
from .ConfigsHandler import Configs
from .DataExportHandlers import GenericAPIv2Handler
from .EntitiesHandler import Entities
from .QueriesHandler import Queries
from .VulnerabilitiesHandler import Vulnerabilities

app = typer.Typer(no_args_is_help=True)

commands = [
    {"command_name": "activities", "command_type": Activities.app},
    {"command_name": "alerts", "command_type": GenericAPIv2Handler.app},
    {"command_name": "configs", "command_type": Configs.app},
    {"command_name": "entities", "command_type": Entities.app},
    {"command_name": "queries", "command_type": Queries.app},
    {"command_name": "vulnerabilities", "command_type": Vulnerabilities.app},
]

for t in common.LegacyV2ObjectTypes:
    commands.append(
        {"command_name": t.value, "command_type": GenericAPIv2Handler.app},
    )

for command in commands:
    app.add_typer(
        command["command_type"],
        name=command["command_name"],
        help=f"Query lacework api {command['command_name']} types",
        no_args_is_help=True,
        epilog=f"{common.config.name} export {command['command_name']} <subtype> <exporttype> [OPTIONS]",
    )

if __name__ == "__main__":
    app()
