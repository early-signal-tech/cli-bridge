"""Run one allowed command: stdin closed, no shell, in its own process group so stop reaches all of it."""

import json
import os
import re
import shutil
import signal
import subprocess
import tempfile
import threading
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from .table import Table, Verb

KILL_AFTER_SECONDS = 5.0
MAX_ERROR_CHARS = 2000
ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")


def default_snapshot_root() -> Path:
    return Path.home() / "Library" / "Application Support" / "Duck Harness" / "Snapshots"


def summarize_stderr(table: Table, raw: bytes) -> str:
    """The tool's error, without colors or its routine warnings."""
    text = ANSI.sub("", raw.decode("utf-8", "replace"))
    lines = [line for line in text.splitlines() if line.strip()]
    if table.stderr_noise:
        lines = [line for line in lines if not table.stderr_noise.search(line)]
    for i, line in enumerate(lines):
        if line.startswith("Error:"):
            lines = lines[i:]
            break
    return "\n".join(lines)[:MAX_ERROR_CHARS]


def save_snapshot(tmp_path: str, root: Path, device: str, verb: str) -> str:
    """Move the raw stdout to <root>/<device>/<time>_<verb>.jsonl. Only this user can read it."""
    folder = root / device
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(folder, 0o700)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = folder / f"{stamp}_{verb}.jsonl"
    n = 1
    while target.exists():  # two of the same verb in one second
        n += 1
        target = folder / f"{stamp}_{verb}_{n}.jsonl"
    shutil.move(tmp_path, target)
    os.chmod(target, 0o600)
    return str(target)


class Job:
    """One running command. stop() can arrive from another thread at any point, even before it starts."""

    def __init__(self):
        self._lock = threading.Lock()
        self._proc: subprocess.Popen | None = None
        self.stopped = False

    def spawn(self, argv: list[str], stdout, stderr) -> subprocess.Popen:
        proc = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr, shell=False,
                                start_new_session=True, env={**os.environ, "NO_COLOR": "1"})
        with self._lock:
            self._proc = proc
            stop_now = self.stopped
        if stop_now:
            self._signal_and_escalate(proc)
        return proc

    def stop(self) -> None:
        with self._lock:
            self.stopped = True
            proc = self._proc
        if proc is not None:
            self._signal_and_escalate(proc)

    def _signal_and_escalate(self, proc: subprocess.Popen) -> None:
        self._signal(proc, signal.SIGTERM)

        def escalate():
            try:
                proc.wait(KILL_AFTER_SECONDS)
            except subprocess.TimeoutExpired:
                self._signal(proc, signal.SIGKILL)

        threading.Thread(target=escalate, daemon=True).start()

    @staticmethod
    def _signal(proc: subprocess.Popen, sig: int) -> None:
        try:
            os.killpg(proc.pid, sig)
        except (ProcessLookupError, PermissionError):
            pass


def _failure(table: Table, stderr_file, exit_code: int) -> dict:
    stderr_file.seek(0)
    error = summarize_stderr(table, stderr_file.read()) or f"{table.tool} exited with {exit_code}"
    return {"ok": False, "error": error, "code": table.error_code(error), "exit": exit_code}


def run_once(table: Table, verb: Verb, binary: str, argv: list[str], job: Job, snapshot_root: Path) -> dict:
    """Run to the end and return its rows. stdout goes to a file, so a big result can't fill a pipe."""
    fd, tmp_path = tempfile.mkstemp(prefix="cli-bridge-", suffix=".jsonl")
    try:
        with os.fdopen(fd, "wb") as stdout, tempfile.TemporaryFile() as stderr:
            proc = job.spawn([binary, *argv], stdout, stderr)
            try:
                exit_code = proc.wait(verb.timeout)
            except subprocess.TimeoutExpired:
                job._signal(proc, signal.SIGKILL)
                proc.wait()
                return {"ok": False, "error": f"{table.tool} took longer than {verb.timeout:g} seconds",
                        "code": "timeout", "exit": None}
            if job.stopped:
                return {"ok": False, "stopped": True, "error": "stopped", "code": "stopped", "exit": exit_code}
            if exit_code != 0:
                return _failure(table, stderr, exit_code)

        rows: deque = deque(maxlen=table.max_rows)
        count = bad_lines = 0
        with open(tmp_path, "rb") as lines:
            for line in lines:
                if not line.strip():
                    continue
                try:
                    rows.append(json.loads(line))
                    count += 1
                except json.JSONDecodeError:
                    bad_lines += 1

        result = {"ok": True, "rows": list(rows), "count": count, "truncated": count > len(rows),
                  "bad_lines": bad_lines, "snapshot": None}
        if verb.snapshot:
            try:
                result["snapshot"] = save_snapshot(tmp_path, snapshot_root, table.device, verb.name)
            except OSError as e:
                # The data is good. A snapshot that can't be saved shouldn't cost the face its result.
                result["snapshot_error"] = str(e)
        return result
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def run_stream(table: Table, verb: Verb, binary: str, argv: list[str], job: Job, on_line) -> dict:
    """Run until the tool exits or stop arrives, sending each line as it comes."""
    with tempfile.TemporaryFile() as stderr:
        proc = job.spawn([binary, *argv], subprocess.PIPE, stderr)
        lines = 0
        for raw in proc.stdout:
            if not raw.strip():
                continue
            lines += 1
            text = raw.decode("utf-8", "replace").rstrip("\r\n")
            try:
                on_line({"line": json.loads(text)})
            except json.JSONDecodeError:
                on_line({"text": text})
        proc.stdout.close()
        exit_code = proc.wait()
        if job.stopped:
            return {"ok": True, "lines": lines, "stopped": True}
        if exit_code != 0:
            return {**_failure(table, stderr, exit_code), "lines": lines, "stopped": False}
        return {"ok": True, "lines": lines, "stopped": False}


def tool_version(table: Table, binary: str) -> str | None:
    """The tool's version, or None if it can't say."""
    try:
        done = subprocess.run([binary, *table.version_argv], stdin=subprocess.DEVNULL, capture_output=True,
                              text=True, timeout=15)
    except (OSError, subprocess.TimeoutExpired):
        return None
    match = table.version_pattern.search(done.stdout + done.stderr)
    return match.group(1) if match else None
