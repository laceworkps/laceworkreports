import typer

from laceworkreports import common

from .AgentCoverageHandler import AgentCoverageHandler
from .ComplianceCoverageHandler import ComplianceCoverageHandler
from .VulnerabilityCoverageHandler import VulnerabilityCoverageHandler

app = typer.Typer(no_args_is_help=True)

commands = [
    {"command_name": "agent-coverage", "command_type": AgentCoverageHandler.app},
    {
        "command_name": "compliance-coverage",
        "command_type": ComplianceCoverageHandler.app,
    },
    {
        "command_name": "vulnerability-coverage",
        "command_type": VulnerabilityCoverageHandler.app,
    },
]

for command in commands:
    app.add_typer(
        command["command_type"],
        name=command["command_name"],
        help=f"Generate {command['command_name']} report",
        no_args_is_help=True,
        epilog=f"{common.config.name} export {command['command_name']} <exporttype> [OPTIONS]",
    )

if __name__ == "__main__":
    app()
