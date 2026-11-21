"""PB-1's engine: Parkbench (the DuckLake benchmark), through its command table.

Parkbench keeps a local catalog's files relative to where it runs (`./ducklake_data`), and
`reset` deletes them there. So the engine runs it in a folder of its own, never wherever the
engine happened to start (a checkout, say).
"""

import os
from pathlib import Path

from ...serve import Handler, serve_table
from ...table import Table, load_table
from . import secrets

TABLE_PATH = Path(__file__).parent / "table.json"


def table() -> Table:
    return load_table(TABLE_PATH)


def default_data_root() -> Path:
    return Path.home() / "Library" / "Application Support" / "Duck Harness" / "Data" / "pb1"


HANDLERS = {
    "secrets_create": Handler(secrets.check, secrets.run),
}


def main() -> None:
    folder = Path(os.environ.get("PB1_DATA_DIR") or default_data_root())
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chdir(folder)
    serve_table(table(), HANDLERS)
