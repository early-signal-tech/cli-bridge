"""PB-1's engine: Parkbench (the DuckLake benchmark), through its command table.

Parkbench keeps a local catalog's files relative to where it runs (`./ducklake_data`), and
`reset` deletes them there. So the engine runs it in a folder of its own, never wherever the
engine happened to start (a checkout, say).

The local DuckDB catalog is reached through a Quack server the engine runs (quack.py), so runs
and PB-1's QUERY terminal can use it at the same time.
"""

import os
from pathlib import Path

from ...serve import Handler, serve_table
from ...table import Table, load_table
from . import secrets
from .quack import DEFAULT_PORT, QuackServer

TABLE_PATH = Path(__file__).parent / "table.json"


def table() -> Table:
    return load_table(TABLE_PATH)


def default_data_root() -> Path:
    return Path.home() / "Library" / "Application Support" / "Duck Harness" / "Data" / "pb1"


def handlers(quack: QuackServer) -> dict[str, Handler]:
    return {
        "secrets_create": Handler(secrets.check, secrets.run),
        "quack_start": Handler(quack.check, quack.run),
    }


def main() -> None:
    folder = Path(os.environ.get("PB1_DATA_DIR") or default_data_root())
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chdir(folder)
    pb = table()
    # Parkbench's own --quack-uri default, so every run attaches where this serves.
    quack = QuackServer(folder, pb.find_binary, port=DEFAULT_PORT)
    quack.environment()
    try:
        serve_table(pb, handlers(quack), prepare=quack.prepare)
    finally:
        quack.stop()
