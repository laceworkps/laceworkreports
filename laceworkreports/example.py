"""Example of code."""
import typer
from laceworksdk import LaceworkClient

app = typer.Typer()


def hello(name: str) -> str:
    """Just an greetings example.

    Args:
        name (str): Name to greet.

    Returns:
        str: greeting message

    Examples:
        .. code:: python

            >>> hello("Roman")
            'Hello Roman!'
    """
    lw = LaceworkClient()
    print(f"test:{lw.account.get_org_info()}")
    return f"Hello {name}!"


if __name__ == "__main__":
    app()
