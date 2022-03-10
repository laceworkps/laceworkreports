import typer

from .ActivitiesHandler import Activities
from .ConfigsHandler import Configs
from .EntitiesHandler import Entities
from .QueriesHandler import Queries
from .VulnerabilitiesHandler import Vulnerabilities

app = typer.Typer()
app.add_typer(Activities.app, name="activities")
app.add_typer(Entities.app, name="entities")
app.add_typer(Queries.app, name="queries")
app.add_typer(Vulnerabilities.app, name="vulnerabilities")
app.add_typer(Configs.app, name="configs")

if __name__ == "__main__":
    app()
