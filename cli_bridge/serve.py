"""The app protocol over JSON Lines on stdin/stdout, for any tool's table. See the README."""

import json
import queue
import re
import sys
import threading
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Callable

from .run import Job, default_snapshot_root, default_work_root, run_login, run_once, run_stream, sweep_work, tool_version
from .table import BadRequest, Table, build_argv

PROTOCOL_VERSION = 1
RESERVED = ("hello", "shutdown", "stop", "secret")
MAX_SECRET_LENGTH = 16_384
# A token has no spaces or line breaks, so it can't add anything to what the terminal receives.
SECRET = re.compile(r"[\x21-\x7e]+")
SHUTDOWN_WAIT_SECONDS = 8.0


@dataclass(frozen=True)
class Handler:
    """A tool's command that needs more than running: `check` validates the request's "input" and
    raises BadRequest, without repeating it. `run` returns the result. Input can hold secrets, so
    neither may log it, and an unexpected failure here is reported without any detail."""

    check: Callable[[Any], Any]
    run: Callable[..., dict]


def bridge_version() -> str:
    try:
        return version("cli-bridge")
    except PackageNotFoundError:
        return "unknown"


def tool_report(table: Table) -> dict:
    """What hello says about the tool. A missing or untested tool still says hello, so the face can explain."""
    report = {"name": table.tool, "path": None, "version": None, "tested": list(table.tested_versions),
              "status": "missing", "warning": None}
    binary = table.find_binary()
    if not binary:
        report["warning"] = f"Can't find {table.tool}. Install it, or set {table.binary_env} to its full path."
        return report
    report["path"] = binary
    found = tool_version(table, binary)
    report["version"] = found
    if found in table.tested_versions:
        report["status"] = "tested"
    elif not table.tested_versions:
        report["status"] = "untested"
        report["warning"] = (f"No {table.tool} version has been marked as tested with this engine yet"
                             f"{f' (found {found})' if found else ''}.")
    else:
        report["status"] = "untested"
        tested = ", ".join(table.tested_versions)
        report["warning"] = f"Tested on {table.tool} {tested}, and found {found or 'an unknown version'}."
    return report


class Server:
    def __init__(self, table: Table, stdin, stdout, snapshot_root: Path | None = None,
                 handlers: dict[str, Handler] | None = None, work_root: Path | None = None):
        self.table, self.stdin, self.stdout = table, stdin, stdout
        self.snapshot_root = snapshot_root or default_snapshot_root()
        self.handlers = handlers or {}
        self.work_root = work_root or default_work_root()
        self._write_lock = threading.Lock()
        self._jobs: dict[str, tuple[Job, threading.Thread]] = {}
        self._jobs_lock = threading.Lock()

    def send(self, message: dict) -> None:
        line = json.dumps(message, default=str)
        with self._write_lock:
            self.stdout.write(line + "\n")
            self.stdout.flush()

    def error(self, request_id, text: str, code: str = "bad_request") -> None:
        self.send({"id": request_id, "type": "error", "error": text, "code": code})

    def handle(self, request) -> bool:
        """Handle one request. False means stop reading."""
        if not isinstance(request, dict):
            self.error(None, "a request must be a JSON object")
            return True
        rid, kind = request.get("id"), request.get("type")
        if not isinstance(rid, str) or not rid:
            self.error(rid, 'every request needs a string "id"')
            return True
        if kind == "hello":
            self.send({"id": rid, "type": "hello", "protocol": PROTOCOL_VERSION, "version": bridge_version(),
                       "device": self.table.device, "tool": tool_report(self.table),
                       "verbs": {name: verb.describe() for name, verb in self.table.verbs.items()}})
        elif kind == "shutdown":
            self.stop_all()
            self.send({"id": rid, "type": "ok"})
            return False
        elif kind == "stop":
            self.stop(rid, request)
        elif kind == "secret":
            self.deliver_secret(rid, request)
        elif kind in self.table.verbs:
            self.start(rid, kind, request)
        else:
            self.error(rid, f"unknown request type: {kind!r}")
        return True

    def stop(self, rid: str, request: dict) -> None:
        target = request.get("request")
        if not isinstance(target, str) or not target:
            self.error(rid, 'stop needs the "request" to stop')
            return
        with self._jobs_lock:
            entry = self._jobs.get(target)
        if entry:
            entry[0].stop()
        # Already finished is fine: its result is on the way or already sent.
        self.send({"id": rid, "type": "ok"})

    def deliver_secret(self, rid: str, request: dict) -> None:
        """Hand a waiting login its secret. Nothing here repeats what it was given."""
        target, secret = request.get("request"), request.get("secret")
        if not isinstance(secret, str) or not secret or len(secret) > MAX_SECRET_LENGTH or not SECRET.fullmatch(secret):
            self.error(rid, "That doesn't look like a token: it should be one line with no spaces.", "bad_secret")
            return
        with self._jobs_lock:
            entry = self._jobs.get(target) if isinstance(target, str) else None
        waiting = entry[0].secrets if entry else None
        if waiting is None:
            self.error(rid, "No login is waiting for a token.", "not_waiting")
            return
        try:
            waiting.put_nowait(secret)
        except queue.Full:
            self.error(rid, "This login already has its token.", "not_waiting")
            return
        self.send({"id": rid, "type": "ok"})

    def start(self, rid: str, kind: str, request: dict) -> None:
        verb = self.table.verbs[kind]
        checked = None
        try:
            argv = build_argv(self.table, verb, request.get("args"), request.get("profile"))
            if verb.mode == "handler":
                if kind not in self.handlers:
                    raise BadRequest(f"{kind} isn't available in this engine")
                checked = self.handlers[kind].check(request.get("input"))
        except BadRequest as e:
            self.error(rid, str(e))
            return
        binary = self.table.find_binary()
        if not binary:
            self.error(rid, f"Can't find {self.table.tool}. Install it, or set {self.table.binary_env} to its "
                            "full path.", "tool_missing")
            return
        with self._jobs_lock:
            if rid in self._jobs:
                self.error(rid, f"request {rid} is already running")
                return
            job = Job()
            thread = threading.Thread(target=self._run, args=(rid, verb, binary, argv, job, checked), daemon=True)
            self._jobs[rid] = (job, thread)
        self.send({"id": rid, "type": "accepted"})
        thread.start()

    def _run(self, rid, verb, binary, argv, job, checked=None) -> None:
        try:
            if verb.mode == "handler":
                result = self._run_handler(verb, binary, argv, job, checked)
            elif verb.mode == "login":
                result = run_login(verb, binary, argv, job, lambda: self.send({"id": rid, "type": "event", "event": "waiting"}))
            elif verb.mode == "stream":
                def on_line(fields):
                    self.send({"id": rid, "type": "event", "event": "line", **fields})
                result = run_stream(self.table, verb, binary, argv, job, on_line, self.snapshot_root)
            else:
                result = run_once(self.table, verb, binary, argv, job, self.snapshot_root)
        except OSError as e:
            result = {"ok": False, "error": f"couldn't run {self.table.tool}: {e}", "code": "tool_error", "exit": None}
        with self._jobs_lock:
            self._jobs.pop(rid, None)
        self.send({"id": rid, "type": "result", "result": result})

    def _run_handler(self, verb, binary, argv, job, checked) -> dict:
        try:
            return self.handlers[verb.name].run(self.table, verb, binary, argv, job, checked, self.work_root)
        except Exception:  # Its details could quote the input, so none of them go anywhere.
            return {"ok": False, "error": f"{verb.name} failed unexpectedly.", "code": "tool_error", "exit": None}

    def stop_all(self) -> None:
        with self._jobs_lock:
            entries = list(self._jobs.values())
        for job, _ in entries:
            job.stop()
        for _, thread in entries:
            if thread.is_alive():
                thread.join(SHUTDOWN_WAIT_SECONDS)

    def serve(self) -> None:
        sweep_work(self.work_root, self.table.device)
        for line in self.stdin:
            if not line.strip():
                continue
            try:
                request = json.loads(line)
            except json.JSONDecodeError as e:
                self.error(None, f"not JSON: {e}")
                continue
            if not self.handle(request):
                return
        self.stop_all()  # end of input


def serve_table(table: Table, handlers: dict[str, Handler] | None = None) -> None:
    Server(table, sys.stdin, sys.stdout, handlers=handlers).serve()
