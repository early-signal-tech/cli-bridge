"""ES-1's draft steps: author, test and delete, always on ES-1's own draft.

flowctl keeps one selected draft per profile, and someone using flowctl at the same time could
change it. So each step selects ES-1's draft by its ID first, in the same run.

Authoring needs the catalog in a file, and a destination's config holds its secrets in plain
text until Estuary encrypts them. So the file is written in a private folder that's always
deleted, as for discover. After authoring, the stored draft is read back: if a secret is still
in it as plain text, the draft is deleted and nothing is returned.
"""

import json
import re
import subprocess
from pathlib import Path

from ...run import Job, private_folder
from ...table import BadRequest, Table, Verb
from .discover import MAX_INPUT_BYTES, _error_text, _forms, _wait, secret_in

DRAFT_ID = re.compile(r"[0-9a-f]{2}(:[0-9a-f]{2}){7}|[0-9a-f]{16}")
STEP_TIMEOUT_SECONDS = 120.0
SPEC_TYPES = ("captures", "collections", "materializations")


def _draft_id(value) -> str:
    if not isinstance(value, str) or not DRAFT_ID.fullmatch(value):
        raise BadRequest("That isn't a draft ID.")
    return value


def check_author(payload) -> dict:
    if not isinstance(payload, dict):
        raise BadRequest('draft_author needs "input": an object of catalog, secret_paths and replace')
    catalog, paths, replace = payload.get("catalog"), payload.get("secret_paths", []), payload.get("replace")
    if not isinstance(catalog, dict) or not catalog or set(catalog) - set(SPEC_TYPES):
        raise BadRequest("The catalog has to hold only captures, collections and materializations.")
    for kind, specs in catalog.items():
        if not isinstance(specs, dict) or any(not isinstance(spec, dict) for spec in specs.values()):
            raise BadRequest(f"The catalog's {kind} have to be objects.")
        for spec in specs.values():
            if "delete" in spec or "reset" in spec:
                raise BadRequest("ES-1 never deletes or resets a live spec.")
    if not isinstance(paths, list) or not all(isinstance(p, list) and all(isinstance(k, str) for k in p) for p in paths):
        raise BadRequest("secret_paths has to be a list of key lists.")
    if len(json.dumps(payload)) > MAX_INPUT_BYTES:
        raise BadRequest("That catalog is too large.")
    secrets = []
    for path in paths:
        value = catalog
        for key in path:
            value = value.get(key) if isinstance(value, dict) else None
        if isinstance(value, str) and value:
            secrets.append(value)
    return {"catalog": catalog, "secrets": sorted(set(secrets), key=len, reverse=True),
            "replace": _draft_id(replace) if replace is not None else None}


def check_draft(payload) -> dict:
    if not isinstance(payload, dict):
        raise BadRequest('This needs "input": an object with draft_id')
    return {"draft_id": _draft_id(payload.get("draft_id"))}


def _step(job: Job, binary: str, profile: list[str], args: list[str], folder: Path, timeout: float) -> tuple[int | None, bytes]:
    with open(folder / "out", "wb") as out, open(folder / "err", "wb") as err:
        code = _wait(job, job.spawn([binary, *profile, *args], out, err, cwd=folder), timeout)
    return code, (folder / "out").read_bytes() + b"\n" + (folder / "err").read_bytes()


def _text_leaks(text: str, secrets: list[str]) -> bool:
    return any(form in text for secret in secrets for form in _forms(secret))


def _spec_leaks(spec, secrets: list[str]) -> bool:
    """A stored spec should be an object. One that comes back as text is read as JSON, and if it
    can't be, it's refused if a secret's text is in it at all."""
    if isinstance(spec, str):
        try:
            spec = json.loads(spec)
        except json.JSONDecodeError:
            return _text_leaks(spec, secrets)
    return secret_in(spec, secrets)


def _profile(verb: Verb, argv: list[str]) -> list[str]:
    return argv[:argv.index(verb.argv[0])]


def _ended(job: Job, code: int | None, timeout: float) -> dict | None:
    if job.stopped and code is not None:
        return {"ok": False, "stopped": True, "error": "stopped", "code": "stopped", "exit": code}
    if code is None:
        return {"ok": False, "error": f"flowctl took longer than {timeout:g} seconds.", "code": "timeout", "exit": None}
    return None


def _failed(table: Table, code: int, printed: bytes, secrets: list[str]) -> dict:
    error = _error_text(table, printed, secrets) or f"{table.tool} exited with {code}"
    return {"ok": False, "error": error, "code": table.error_code(error), "exit": code}


def _rows(printed: bytes) -> list[dict]:
    rows = []
    for line in printed.decode("utf-8", "replace").splitlines():
        if line.startswith("{"):
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return rows


def run_author(table: Table, verb: Verb, binary: str, argv: list[str], job: Job, checked: dict, work_root: Path) -> dict:
    """A fresh draft holding this catalog. One it replaces is deleted first, so nothing stale is left."""
    profile, secrets = _profile(verb, argv), checked["secrets"]
    with private_folder(work_root, table.device) as folder:
        if checked["replace"]:
            for args in (["draft", "select", "--id", checked["replace"]], ["draft", "delete"]):
                code, printed = _step(job, binary, profile, args, folder, STEP_TIMEOUT_SECONDS)
                if (ended := _ended(job, code, STEP_TIMEOUT_SECONDS)) or code != 0:
                    break  # The old one may be gone already; a new one is made either way.
            if job.stopped:
                return _ended(job, 0, STEP_TIMEOUT_SECONDS)
        code, printed = _step(job, binary, profile, ["draft", "create", "-o", "json"], folder, STEP_TIMEOUT_SECONDS)
        if ended := _ended(job, code, STEP_TIMEOUT_SECONDS):
            return ended
        if code != 0:
            return _failed(table, code, printed, secrets)
        created = _rows(printed)
        draft_id = created[0].get("id") if created else None
        if not isinstance(draft_id, str) or not DRAFT_ID.fullmatch(draft_id):
            return {"ok": False, "error": "flowctl made a draft but didn't say which.", "code": "tool_error", "exit": 0}

        def give_up(result: dict) -> dict:
            # Nothing half-made stays behind, especially a draft that might hold a secret. A stop
            # or timeout has stopped `job`, so cleaning up runs on a job of its own.
            cleanup = Job()
            _step(cleanup, binary, profile, ["draft", "select", "--id", draft_id], folder, STEP_TIMEOUT_SECONDS)
            _step(cleanup, binary, profile, ["draft", "delete"], folder, STEP_TIMEOUT_SECONDS)
            return result

        (folder / "draft.json").touch(mode=0o600)
        (folder / "draft.json").write_text(json.dumps(checked["catalog"]))
        code, printed = _step(job, binary, [*profile], [*verb.argv[:2], "--source", "draft.json"], folder, verb.timeout)
        (folder / "draft.json").unlink()
        if ended := _ended(job, code, verb.timeout):
            return give_up(ended)
        if code != 0:
            return give_up(_failed(table, code, printed, secrets))

        code, printed = _step(job, binary, profile, ["raw", "get", "--table", "draft_specs", "--query", "select=catalog_name,spec_type,spec",
                                                     "--query", f"draft_id=eq.{draft_id}"], folder, STEP_TIMEOUT_SECONDS)
        stored = printed.decode("utf-8", "replace")
        if code != 0:
            return give_up({"ok": False, "error": "flowctl drafted the specs but couldn't read them back to check them.", "code": "tool_error", "exit": code})
        try:
            specs = json.JSONDecoder().raw_decode(stored[stored.index("["):])[0]
        except ValueError:
            specs = None
        # Values only, as for discover. What can't be read is refused if a secret's text is in it at all.
        leaked = (any(_spec_leaks(s.get("spec") if isinstance(s, dict) else s, secrets) for s in specs) if isinstance(specs, list)
                  else _text_leaks(stored, secrets))
        if leaked:
            return give_up({"ok": False, "code": "secret_left", "exit": 0,
                            "error": "Estuary kept a secret unencrypted, so the draft was deleted. Please check your flowctl version."})
    specs = specs if isinstance(specs, list) else []
    return {"ok": True, "draft_id": draft_id,
            "specs": [{"catalog_name": s.get("catalog_name"), "spec_type": s.get("spec_type")} for s in specs if isinstance(s, dict)]}


def _on_draft(table: Table, verb: Verb, binary: str, argv: list[str], job: Job, draft_id: str, work_root: Path) -> dict:
    """Select ES-1's draft, then run this verb's command on it."""
    profile = _profile(verb, argv)
    with private_folder(work_root, table.device) as folder:
        code, printed = _step(job, binary, profile, ["draft", "select", "--id", draft_id], folder, STEP_TIMEOUT_SECONDS)
        if ended := _ended(job, code, STEP_TIMEOUT_SECONDS):
            return ended
        if code != 0:
            return {"ok": False, "error": "That draft isn't there any more. Press DRAFT to make a new one.", "code": "no_draft", "exit": code}
        code, printed = _step(job, binary, [], argv, folder, verb.timeout)
        if ended := _ended(job, code, verb.timeout):
            return ended
        if code != 0:
            return _failed(table, code, printed, [])
    return {"ok": True, "draft_id": draft_id}


def run_test(table, verb, binary, argv, job, checked, work_root) -> dict:
    return _on_draft(table, verb, binary, argv, job, checked["draft_id"], work_root)


def run_delete(table, verb, binary, argv, job, checked, work_root) -> dict:
    return _on_draft(table, verb, binary, argv, job, checked["draft_id"], work_root)
