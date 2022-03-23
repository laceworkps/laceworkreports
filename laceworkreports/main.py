"""
CLI Main
"""
from types import SimpleNamespace

import typer
from rich.console import Console

from laceworkreports import common, version
from laceworkreports.cli.ExportHandlers import Export
from laceworkreports.cli.ReportHandlers import Report

app = typer.Typer(
    name=common.config.name,
    help=f"{common.config.name} is a Python cli/package for exporting and creating reports from Lacework data.",
    add_completion=True,
    no_args_is_help=True,
    epilog=f"{common.config.name} <action> <type> <subtype> <exporttype> [OPTIONS]",
)

parent_command = common.config.name
self_command = common.ActionTypes.Export.value

commands = [
    {
        "command_name": "export",
        "command_type": Export.app,
        "epilog": f"{common.config.name} export <type> <subtype> <exporttype> [OPTIONS]",
    },
    {
        "command_name": "report",
        "command_type": Report.app,
        "epilog": f"{common.config.name} report <type> <format> [OPTIONS]",
    },
]

for command in iter(commands):
    app.add_typer(
        command["command_type"],
        name=command["command_name"],
        help=f"{command['command_name'].capitalize()} lacework events",
        no_args_is_help=True,
        epilog=command["epilog"],
    )

console = Console()


def version_callback(ctx: typer.Context, print_version: bool) -> None:
    """Print the version of the package."""
    if print_version:
        console.print(f"[yellow]laceworkreports[/] version: [bold blue]{version}[/]")
        raise typer.Exit()


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    version: bool = typer.Option(None, "--version", callback=version_callback),
    account: str = typer.Option(
        None,
        envvar=common.LACEWORK_ACCOUNT_ENVIRONMENT_VARIABLE,
        help="account subdomain of URL (i.e. <ACCOUNT>.lacework.net)",
    ),
    subaccount: str = typer.Option(
        None,
        envvar=common.LACEWORK_SUBACCOUNT_ENVIRONMENT_VARIABLE,
        help="sub-account name inside your organization (org admins only)",
    ),
    api_key: str = typer.Option(
        None, envvar=common.LACEWORK_API_KEY_ENVIRONMENT_VARIABLE, help="access key id"
    ),
    api_secret: str = typer.Option(
        None,
        envvar=common.LACEWORK_API_SECRET_ENVIRONMENT_VARIABLE,
        help="secret access key",
    ),
    # instance: str = typer.Option(None, envvar=common.LACEWORK_ACCOUNT_ENVIRONMENT_VARIABLE, help=""),
    profile: str = typer.Option(
        None, help="switch between profiles configured at ~/.lacework.toml"
    ),
    base_domain: str = typer.Option(
        None,
        envvar=common.LACEWORK_API_BASE_DOMAIN_ENVIRONMENT_VARIABLE,
        help="lacework.net or fra.lacework.net (default: lacework.net)",
    ),
    sample: bool = typer.Option(
        common.config.sample,
        help="print first row of response from api and exit",
    ),
) -> None:
    """
    Set the search context for the LaceworkClient
    """

    # lacework client context
    instance = account
    common.config.account = account
    common.config.subaccount = subaccount
    common.config.api_key = api_key
    common.config.api_secret = api_secret
    common.config.instance = instance
    common.config.profile = profile
    common.config.base_domain = base_domain
    common.config.sample = sample

    ctx.obj = SimpleNamespace(
        account=account,
        subaccount=subaccount,
        api_key=api_key,
        api_secret=api_secret,
        instance=instance,
        profile=profile,
        base_domain=base_domain,
    )


if __name__ == "__main__":
    app()
