"""ES-1's engine: flowctl, through its command table."""

from pathlib import Path

from ...serve import Handler, serve_table
from ...table import Table, load_table
from . import discover

TABLE_PATH = Path(__file__).parent / "table.json"


def table() -> Table:
    return load_table(TABLE_PATH)


HANDLERS = {"discover": Handler(discover.check, discover.run)}


def main() -> None:
    serve_table(table(), HANDLERS)
