from types import SimpleNamespace

import typer
from rich.console import Console

from laceworkreports import common, version

from .cli.ExportHandler import Export

app = typer.Typer(
    name="laceworkreports",
    help="laceworkreports is a Python cli/package for creating reports from Lacework data.",
    add_completion=True,
)
app.add_typer(Export.app, name="export")
console = Console()


def version_callback(ctx: typer.Context, print_version: bool) -> None:
    """Print the version of the package."""
    if print_version:
        console.print(f"[yellow]laceworkreports[/] version: [bold blue]{version}[/]")
        raise typer.Exit()


@app.callback()
def main(
    ctx: typer.Context,
    version: bool = typer.Option(None, "--version", callback=version_callback),
    account: str = typer.Option(None),
    subaccount: str = typer.Option(None),
    api_key: str = typer.Option(None),
    api_secret: str = typer.Option(None),
    instance: str = typer.Option(None),
    profile: str = typer.Option(None),
    base_domain: str = typer.Option(None),
) -> None:
    """
    Set the search context for the LaceworkClient
    """

    # lacework client context
    common.config.account = account
    common.config.subaccount = subaccount
    common.config.api_key = api_key
    common.config.api_secret = api_secret
    common.config.instance = instance
    common.config.profile = profile
    common.config.base_domain = base_domain

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
