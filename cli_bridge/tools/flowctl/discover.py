"""`flowctl discover`: the one command here that has to hold real secrets on disk.

flowctl only reads a spec from a file, so the connector's config, passwords included, has to be
written down for as long as discover runs. On success flowctl replaces the secrets with encrypted
ones. On failure the plain text stays in that file. So:

- It runs in a private folder that's always deleted, however the run ends. A crash can't delete
  anything, so what one leaves behind is swept by the next engine and the next discover.
- Nothing it prints is passed along without the secrets taken out.
- What comes back is checked. A secret in the result, or a result that isn't encrypted, is
  refused and nothing is returned.
"""

import json
import re
import subprocess
from pathlib import Path

from ...run import ANSI, Job, private_folder, summarize_stderr
from ...table import BadRequest, Table, Verb

MAX_INPUT_BYTES = 1_000_000
MAX_ERROR_CHARS = 1000
BUNDLE_TIMEOUT_SECONDS = 60.0
HIDDEN = "[hidden]"
CATALOG_NAME = re.compile(r"(?!.*\.\.)[^\W][\w.-]*(/[\w.-]+)+")
IMAGE = re.compile(r"ghcr\.io/estuary/source-(postgres|mysql|sqlserver|mongodb|snowflake):v\d+")


def check(payload) -> dict:
    """What discover needs, or BadRequest. No message here repeats anything it was given."""
    if not isinstance(payload, dict):
        raise BadRequest('discover needs "input": an object of name, image, config and secret_paths')
    name, image, config, paths = (payload.get(k) for k in ("name", "image", "config", "secret_paths"))
    if not isinstance(name, str) or not CATALOG_NAME.fullmatch(name):
        raise BadRequest("The name has to look like prefix/name.")
    if not isinstance(image, str) or not IMAGE.fullmatch(image):
        raise BadRequest("That connector isn't one ES-1 can discover.")
    if not isinstance(config, dict):
        raise BadRequest("The connector's config has to be an object.")
    if not isinstance(paths, list) or not all(isinstance(p, str) for p in paths):
        raise BadRequest("secret_paths has to be a list of paths.")
    if len(json.dumps(payload)) > MAX_INPUT_BYTES:
        raise BadRequest("That config is too large.")
    return {"name": name, "image": image, "config": config, "secrets": _secrets(config, paths)}


def _secrets(config: dict, paths: list[str]) -> list[str]:
    found = []
    for path in paths:
        value = config
        for part in path.split("."):
            value = value.get(part) if isinstance(value, dict) else None
        if isinstance(value, str) and value:
            found.append(value)
    # Longest first, so taking one out can't leave part of a longer one behind.
    return sorted(set(found), key=len, reverse=True)


def _forms(secret: str) -> list[str]:
    """How a secret can appear in text: as it is, and with JSON's escaping."""
    return list({secret, json.dumps(secret)[1:-1]} | set(line for line in secret.splitlines() if len(line) > 3))


def secret_in(value, secrets: list[str], in_config: bool = False) -> bool:
    """Whether a secret is still in this JSON value, as plain text.

    Only values are checked, never keys: a password of "password" isn't a leak because the
    encrypted field is called "password_sops". Inside a connector's config, where secrets live,
    any value that contains one counts, such as a connection URI. Elsewhere (schemas, resources),
    only a value that is the secret, since a common word could be in a description.
    """
    if isinstance(value, dict):
        return any(secret_in(v, secrets, in_config or k == "config") for k, v in value.items())
    if isinstance(value, list):
        return any(secret_in(v, secrets, in_config) for v in value)
    if isinstance(value, str):
        return any(value == s or (in_config and s in value) for s in secrets)
    return False


def hide(text: str, secrets: list[str]) -> str:
    for secret in secrets:
        for form in sorted(_forms(secret), key=len, reverse=True):
            text = text.replace(form, HIDDEN)
    return text


def _error_text(table: Table, raw: bytes, secrets: list[str]) -> str:
    """The connector's own ERROR lines say what's wrong, so they come first."""
    plain = hide(ANSI.sub("", raw.decode("utf-8", "replace")), secrets)
    errors = [re.sub(r"^.*?\bERROR:\s*", "", line) for line in plain.splitlines() if " ERROR: " in line]
    return "\n".join(errors)[:MAX_ERROR_CHARS] or hide(summarize_stderr(table, raw), secrets)[:MAX_ERROR_CHARS]


def _wait(job: Job, proc: subprocess.Popen, timeout: float) -> int | None:
    try:
        return proc.wait(timeout)
    except subprocess.TimeoutExpired:
        job.stop()
        proc.wait()
        return None


def run(table: Table, verb: Verb, binary: str, argv: list[str], job: Job, checked: dict, work_root: Path) -> dict:
    secrets = checked["secrets"]
    profile = argv[:argv.index(verb.argv[0])]
    spec = {"captures": {checked["name"]: {
        "endpoint": {"connector": {"image": checked["image"], "config": checked["config"]}},
        "bindings": [],
    }}}
    with private_folder(work_root, table.device) as folder:
        (folder / "flow.yaml").touch(mode=0o600)
        (folder / "flow.yaml").write_text(json.dumps(spec))  # JSON is YAML.
        with open(folder / "out", "wb") as out, open(folder / "err", "wb") as err:
            code = _wait(job, job.spawn([binary, *argv], out, err, cwd=folder), verb.timeout)
        if job.stopped and code is not None:
            return {"ok": False, "stopped": True, "error": "stopped", "code": "stopped", "exit": code}
        if code is None:
            return {"ok": False, "error": f"Discover took longer than {verb.timeout:g} seconds.", "code": "timeout", "exit": None}
        if code != 0:
            # The connector's log goes to stdout, and flowctl's own summary to stderr.
            printed = (folder / "out").read_bytes() + b"\n" + (folder / "err").read_bytes()
            error = _error_text(table, printed, secrets) or f"{table.tool} exited with {code}"
            return {"ok": False, "error": error, "code": table.error_code(error), "exit": code}

        with open(folder / "bundle", "wb") as out, open(folder / "err", "wb") as err:
            bundle = job.spawn([binary, *profile, "raw", "bundle", "--source", "flow.yaml", "-o", "json"], out, err, cwd=folder)
            code = _wait(job, bundle, BUNDLE_TIMEOUT_SECONDS)
        if code != 0:
            return {"ok": False, "error": "flowctl found the tables but couldn't read back what it found.", "code": "tool_error", "exit": code}
        text = (folder / "bundle").read_text(encoding="utf-8", errors="replace")

    # Only the bundle comes back, so it's the one thing that has to be clean.
    refused = {"ok": False, "code": "secret_left", "exit": 0,
               "error": "flowctl didn't encrypt the secrets as expected, so nothing was kept. Please check your flowctl version."}
    try:
        catalog = json.loads(text)
        capture = catalog["captures"][checked["name"]]
        config = capture["endpoint"]["connector"]["config"]
        bindings = capture["bindings"]
    except (json.JSONDecodeError, KeyError, TypeError):
        # Unreadable, so it can't be checked: refuse it if a secret's text is anywhere in it.
        if any(form in text for secret in secrets for form in _forms(secret)):
            return refused
        return {"ok": False, "error": "flowctl found the tables but couldn't read back what it found.", "code": "tool_error", "exit": 0}
    if secret_in(catalog, secrets) or (secrets and not (isinstance(config, dict) and "sops" in config)):
        return refused
    return {"ok": True, "catalog": catalog, "bindings": [
        {"target": b.get("target"), "resource": b.get("resource")} for b in bindings if isinstance(b, dict)
    ]}
