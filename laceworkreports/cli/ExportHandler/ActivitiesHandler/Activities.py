import typer

from laceworkreports import common
from laceworkreports.cli.ExportHandler.GenericHandler import GenericAPIv2Handler

app = typer.Typer()

for t in common.ActivitiesTypes:
    app.add_typer(GenericAPIv2Handler.app, name=t.value)


if __name__ == "__main__":
    app()
