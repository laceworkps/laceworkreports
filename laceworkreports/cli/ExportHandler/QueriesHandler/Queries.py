import typer

from laceworkreports import common
from laceworkreports.cli.ExportHandler.GenericHandler import GenericLQLHandler

app = typer.Typer()

for t in common.QueriesTypes:
    app.add_typer(GenericLQLHandler.app, name=t.value)


if __name__ == "__main__":
    app()
