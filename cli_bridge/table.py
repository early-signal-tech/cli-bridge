"""A tool's command table: the only commands an engine may run, and how to check their arguments."""

import json
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

MAX_VALUE_LENGTH = 512
MAX_VALUES = 100
DEFAULT_TIMEOUT_SECONDS = 120.0
DEFAULT_MAX_ROWS = 20_000
MODES = ("once", "stream", "login")


class BadRequest(Exception):
    """A request the table doesn't allow. Nothing has been run."""


@dataclass(frozen=True)
class Arg:
    name: str
    type: str
    pattern: re.Pattern
    flag: str | None = None  # None means positional
    many: bool = False
    required: bool = False
    default: str | None = None


@dataclass(frozen=True)
class SnapshotRule:
    """What a verb's snapshot may hold. Only what's named here is saved, so a field the tool
    adds later stays out until someone adds it. `raw` saves stdout as it came, for output
    that can't carry secrets or customer data."""

    raw: bool = False
    keep: tuple[str, ...] = ()  # dotted paths, copied whole
    first_line: tuple[str, ...] = ()  # text: its first line, cut short
    numbers: tuple[str, ...] = ()  # objects: only their numeric values


@dataclass(frozen=True)
class Verb:
    name: str
    argv: tuple[str, ...]
    mode: str
    args: tuple[Arg, ...]
    snapshot: SnapshotRule | None  # None: never saved
    timeout: float

    def describe(self) -> dict:
        """What hello tells the face about this verb."""
        return {
            "mode": self.mode,
            "args": {
                a.name: {"type": a.type, "many": a.many, "required": a.required, "default": a.default}
                for a in self.args
            },
        }


@dataclass(frozen=True)
class Table:
    device: str
    tool: str
    binary: str
    binary_env: str
    fallback_paths: tuple[str, ...]
    version_argv: tuple[str, ...]
    version_pattern: re.Pattern
    tested_versions: tuple[str, ...]
    profile_flag: str | None
    profile_pattern: re.Pattern | None
    max_rows: int
    stderr_noise: re.Pattern | None
    error_codes: tuple[tuple[str, str], ...]
    verbs: dict[str, Verb]

    def find_binary(self) -> str | None:
        """The tool's path: the override, then the path, then the usual places.

        An app launched from the Finder has a short path, so the usual places matter.
        """
        override = os.environ.get(self.binary_env)
        if override:
            return override if os.access(override, os.X_OK) else None
        found = shutil.which(self.binary)
        if found:
            return found
        for path in self.fallback_paths:
            if os.access(path, os.X_OK):
                return path
        return None

    def error_code(self, text: str) -> str:
        for contains, code in self.error_codes:
            if contains in text:
                return code
        return "tool_error"


def _load_snapshot_rule(verb: str, spec: dict) -> SnapshotRule | None:
    """Every verb has to say what it saves: leaving it out is an error, not a raw snapshot."""
    if "snapshot" not in spec:
        raise ValueError(f"verb {verb}: needs a snapshot rule, or \"snapshot\": false")
    rule = spec["snapshot"]
    if rule is False:
        return None
    if spec["mode"] == "login":
        raise ValueError(f"verb {verb}: a login is never saved, so its snapshot has to be false")
    if not isinstance(rule, dict) or set(rule) - {"raw", "keep", "first_line", "numbers"}:
        raise ValueError(f"verb {verb}: a snapshot rule is false, or an object of raw, keep, first_line, numbers")
    if rule.get("raw") is True:
        if len(rule) > 1 or spec["mode"] == "stream":
            raise ValueError(f"verb {verb}: raw stands alone, and a stream can't be saved raw")
        return SnapshotRule(raw=True)
    parsed = SnapshotRule(keep=tuple(rule.get("keep", ())), first_line=tuple(rule.get("first_line", ())),
                          numbers=tuple(rule.get("numbers", ())))
    if not (parsed.keep or parsed.first_line or parsed.numbers):
        raise ValueError(f"verb {verb}: a snapshot rule has to keep something")
    return parsed


def load_table(path: str | Path) -> Table:
    raw = json.loads(Path(path).read_text())
    types = {name: re.compile(pattern) for name, pattern in raw.get("types", {}).items()}
    verbs = {}
    for name, spec in raw["verbs"].items():
        if spec["mode"] not in MODES:
            raise ValueError(f"verb {name}: unknown mode {spec['mode']!r}")
        args = []
        for arg_name, a in spec.get("args", {}).items():
            if a["type"] not in types:
                raise ValueError(f"verb {name}: argument {arg_name} has unknown type {a['type']!r}")
            default = a.get("default")
            if default is not None and not types[a["type"]].fullmatch(default):
                raise ValueError(f"verb {name}: default for {arg_name} doesn't match its type")
            args.append(Arg(arg_name, a["type"], types[a["type"]], a.get("flag"), a.get("many", False),
                            a.get("required", False), default))
        verbs[name] = Verb(name, tuple(spec["argv"]), spec["mode"], tuple(args), _load_snapshot_rule(name, spec),
                           float(spec.get("timeout", DEFAULT_TIMEOUT_SECONDS)))
    profile_type = raw.get("profile_type")
    noise = raw.get("stderr_noise")
    return Table(
        device=raw["device"],
        tool=raw["tool"],
        binary=raw["binary"],
        binary_env=raw["binary_env"],
        fallback_paths=tuple(raw.get("fallback_paths", [])),
        version_argv=tuple(raw["version_argv"]),
        version_pattern=re.compile(raw["version_pattern"]),
        tested_versions=tuple(raw["tested_versions"]),
        profile_flag=raw.get("profile_flag"),
        profile_pattern=types[profile_type] if profile_type else None,
        max_rows=int(raw.get("max_rows", DEFAULT_MAX_ROWS)),
        stderr_noise=re.compile(noise) if noise else None,
        error_codes=tuple((e["contains"], e["code"]) for e in raw.get("error_codes", [])),
        verbs=verbs,
    )


def _check_value(label: str, value, pattern: re.Pattern) -> str:
    if not isinstance(value, str) or not value:
        raise BadRequest(f"{label} must be a non-empty string")
    if len(value) > MAX_VALUE_LENGTH:
        raise BadRequest(f"{label} is too long")
    # The one rule every tool shares: a value can never become a flag.
    if value.startswith("-"):
        raise BadRequest(f"{label} can't start with '-'")
    if not pattern.fullmatch(value):
        raise BadRequest(f"{label} isn't valid: {value!r}")
    return value


def build_argv(table: Table, verb: Verb, args: dict | None = None, profile: str | None = None) -> list[str]:
    """The arguments after the binary, or BadRequest. Only the table's words and checked values get in."""
    args = {} if args is None else args
    if not isinstance(args, dict):
        raise BadRequest('"args" must be an object')
    known = {a.name for a in verb.args}
    unknown = sorted(set(args) - known)
    if unknown:
        raise BadRequest(f"{verb.name} doesn't take: {', '.join(unknown)}")

    argv: list[str] = []
    if profile is not None:
        if not table.profile_flag or not table.profile_pattern:
            raise BadRequest(f"{table.tool} has no profiles")
        argv += [table.profile_flag, _check_value("profile", profile, table.profile_pattern)]
    argv += verb.argv

    for arg in verb.args:
        value = args.get(arg.name, arg.default)
        if value is None:
            if arg.required:
                raise BadRequest(f"{verb.name} needs {arg.name}")
            continue
        if arg.many:
            if not isinstance(value, list) or not value:
                raise BadRequest(f"{arg.name} must be a non-empty list")
            if len(value) > MAX_VALUES:
                raise BadRequest(f"{arg.name} has too many values")
            values = value
        else:
            values = [value]
        for v in values:
            checked = _check_value(arg.name, v, arg.pattern)
            argv += [arg.flag, checked] if arg.flag else [checked]
    return argv
