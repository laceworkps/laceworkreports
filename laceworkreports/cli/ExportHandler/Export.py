import typer

from .ActivitiesHandler import Activities
from .EntitiesHandler import Entities
from .QueriesHandler import Queries
from .VulnerabilitiesHandler import Vulnerabilities

app = typer.Typer()
app.add_typer(Activities.app, name="activities")
app.add_typer(Entities.app, name="entities")
app.add_typer(Queries.app, name="queries")
app.add_typer(Vulnerabilities.app, name="vulnerabilities")

if __name__ == "__main__":
    app()
