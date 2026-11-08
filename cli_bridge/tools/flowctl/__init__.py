"""ES-1's engine: flowctl, through its command table."""

from pathlib import Path

from ...serve import serve_table
from ...table import Table, load_table

TABLE_PATH = Path(__file__).parent / "table.json"


def table() -> Table:
    return load_table(TABLE_PATH)


def main() -> None:
    serve_table(table())
