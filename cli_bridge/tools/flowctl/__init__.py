"""ES-1's engine: flowctl, through its command table."""

from pathlib import Path

from ...serve import Handler, serve_table
from ...table import Table, load_table
from . import discover, drafts

TABLE_PATH = Path(__file__).parent / "table.json"


def table() -> Table:
    return load_table(TABLE_PATH)


HANDLERS = {
    "discover": Handler(discover.check, discover.run),
    "draft_author": Handler(drafts.check_author, drafts.run_author),
    "draft_test": Handler(drafts.check_draft, drafts.run_test),
    "draft_delete": Handler(drafts.check_draft, drafts.run_delete),
    "draft_publish": Handler(drafts.check_publish, drafts.run_publish),
}


def main() -> None:
    serve_table(table(), HANDLERS)
