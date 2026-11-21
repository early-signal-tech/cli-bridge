"""PB-1's secrets_create: a DuckDB secret's values go to Parkbench on stdin, never as arguments.

`parkbench secrets create --type T --name N` reads one JSON object of values from stdin. So the
values never reach an argument, a file, a log, a snapshot or a message back. Parkbench's own
errors name a field and never a value. Its message is still checked here: if it holds a
password, secret or key at all, none of it is passed on.
"""

import json
import subprocess
import tempfile

from ...run import Job, summarize_stderr
from ...table import BadRequest, Table, Verb

MAX_INPUT_BYTES = 16_384
MAX_VALUES = 16
# The fields of Parkbench's secret types that hold credentials (parkbench secrets_input.go).
SENSITIVE = ("password", "secret", "key_id")
WITHHELD = "parkbench refused the secret, and its message is withheld because it quoted a value. Check the values and try again."


def check(payload) -> dict:
    """`input` is {"values": {field: text}}. Nothing here repeats a value."""
    values = payload.get("values") if isinstance(payload, dict) else None
    if not isinstance(values, dict) or not values or set(payload) != {"values"}:
        raise BadRequest('secrets_create needs "input": {"values": {...}}')
    if len(values) > MAX_VALUES:
        raise BadRequest("That's too many values for one secret.")
    for key, value in values.items():
        if not isinstance(key, str) or not key.isidentifier():
            raise BadRequest("A value's name has to be a field name.")
        if not isinstance(value, str):
            raise BadRequest(f"{key} has to be text.")
    encoded = json.dumps(values)
    if len(encoded) > MAX_INPUT_BYTES:
        raise BadRequest("Those values are too long.")
    return {"stdin": encoded.encode(), "sensitive": [v for k, v in values.items() if k in SENSITIVE and v]}


def _safe_error(text: str, sensitive: list[str]) -> str:
    """The tool's message, or none of it if a credential is anywhere in it."""
    return WITHHELD if any(value in text for value in sensitive) else text


def run(table: Table, verb: Verb, binary: str, argv: list[str], job: Job, checked: dict, work_root) -> dict:
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        proc = job.spawn([binary, *argv], out, err, stdin=subprocess.PIPE)
        try:
            proc.stdin.write(checked["stdin"])
            proc.stdin.close()
        except BrokenPipeError:  # It exited before reading: its stderr says why.
            pass
        try:
            code = proc.wait(verb.timeout)
        except subprocess.TimeoutExpired:
            job.stop()
            proc.wait()
            return {"ok": False, "error": f"parkbench took longer than {verb.timeout:g} seconds", "code": "timeout", "exit": None}
        if job.stopped:
            return {"ok": False, "stopped": True, "error": "stopped", "code": "stopped", "exit": code}
        if code != 0:
            err.seek(0)
            text = _safe_error(summarize_stderr(table, err.read()), checked["sensitive"]) or f"parkbench exited with {code}"
            return {"ok": False, "error": text, "code": table.error_code(text), "exit": code}
        return {"ok": True, "exit": 0}
