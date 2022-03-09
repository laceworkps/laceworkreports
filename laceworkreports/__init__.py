"""laceworkreports is a Python cli/package for creating reports from Lacework data."""

import importlib_metadata


def get_version() -> str:
    try:
        return str(importlib_metadata.version(__name__))
    except importlib_metadata.PackageNotFoundError:  # pragma: no cover
        return "unknown"


version: str = get_version()
