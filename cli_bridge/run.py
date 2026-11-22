"""Run one allowed command: stdin closed, no shell, in its own process group so stop reaches all of it."""

import json
import fcntl
import os
import pty
import queue
import re
import select
import shutil
import signal
import subprocess
import tempfile
import termios
import threading
import time
from collections import deque
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .table import SnapshotRule, Table, Verb

KILL_AFTER_SECONDS = 5.0
MAX_ERROR_CHARS = 2000
MAX_SNAPSHOT_TEXT = 200
STALE_WORK_SECONDS = 3600.0
LOGIN_POLL_SECONDS = 0.1
LOGIN_EXIT_WAIT_SECONDS = 5.0
LOGIN_REFUSED = "That token wasn't accepted, so nothing was saved. Copy a new one from the token page and try again."
ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")


def default_snapshot_root() -> Path:
    return Path.home() / "Library" / "Application Support" / "Duck Harness" / "Snapshots"


def default_work_root() -> Path:
    return Path.home() / "Library" / "Application Support" / "Duck Harness" / "Work"


def sweep_work(root: Path, device: str, older_than: float = STALE_WORK_SECONDS) -> None:
    """Delete what a crashed run left behind. A run that's still going is younger than this."""
    folder = root / device
    if not folder.is_dir():
        return
    for path in folder.iterdir():
        try:
            if time.time() - path.stat().st_mtime > older_than:
                shutil.rmtree(path, ignore_errors=True)
        except OSError:
            pass


@contextmanager
def private_folder(root: Path, device: str):
    """A folder only this user can read, deleted however the block ends."""
    parent = root / device
    parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(parent, 0o700)
    sweep_work(root, device)
    folder = Path(tempfile.mkdtemp(prefix="run-", dir=parent))
    try:
        yield folder
    finally:
        shutil.rmtree(folder, ignore_errors=True)


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


_MISSING = object()


def _get(row: dict, path: str):
    value = row
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return _MISSING
        value = value[part]
    return value


def _put(row: dict, path: str, value) -> None:
    *parents, last = path.split(".")
    for part in parents:
        row = row.setdefault(part, {})
    row[last] = value


def trim_row(row, rule: SnapshotRule) -> dict | None:
    """Only what the rule names, or None if that's nothing."""
    if not isinstance(row, dict):
        return None
    out: dict = {}
    for path in rule.keep:
        value = _get(row, path)
        if value is not _MISSING:
            _put(out, path, value)
    for path in rule.first_line:
        value = _get(row, path)
        if isinstance(value, str):
            lines = value.splitlines()
            _put(out, path, lines[0][:MAX_SNAPSHOT_TEXT] if lines else "")
    for path in rule.numbers:
        value = _get(row, path)
        if isinstance(value, dict):
            numbers = {k: v for k, v in value.items() if isinstance(v, (int, float)) and not isinstance(v, bool)}
            if numbers:
                _put(out, path, numbers)
    return out or None


def _open_snapshot(root: Path, device: str, verb: str):
    """A new <root>/<device>/<time>_<verb>.jsonl. Only this user can read it, from the start."""
    folder = root / device
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(folder, 0o700)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    n = 1
    while True:
        name = f"{stamp}_{verb}.jsonl" if n == 1 else f"{stamp}_{verb}_{n}.jsonl"
        target = folder / name
        try:
            fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:  # two of the same verb in one second
            n += 1
            continue
        return target, os.fdopen(fd, "w", encoding="utf-8")


class TrimmedSnapshot:
    """Writes rows through a rule as they arrive. A snapshot that fails shouldn't cost the face
    its result, so it deletes what it wrote and says why in `error`."""

    def __init__(self, rule: SnapshotRule, root: Path, device: str, verb: str):
        self.rule, self.path, self.file, self.error = rule, None, None, None
        try:
            self.path, self.file = _open_snapshot(root, device, verb)
        except OSError as e:
            self.error = str(e)

    def add(self, row) -> None:
        trimmed = trim_row(row, self.rule)
        if self.file is None or trimmed is None:
            return
        try:
            self.file.write(json.dumps(trimmed) + "\n")
            self.file.flush()  # A stream can run for days: what's arrived should survive a crash.
        except OSError as e:
            self.error = str(e)
            self._drop()

    def finish(self, keep_empty: bool = True) -> str | None:
        """Close it. Its path, or None if there's nothing to show for it."""
        if self.file is None:
            return None
        try:
            self.file.close()
            self.file = None
        except OSError as e:
            self.error = str(e)
            self._drop()
            return None
        if not keep_empty and os.path.getsize(self.path) == 0:
            os.unlink(self.path)
            return None
        return str(self.path)

    def _drop(self) -> None:
        if self.file is not None:
            try:
                self.file.close()
            except OSError:
                pass
            self.file = None
        if self.path is not None and os.path.exists(self.path):
            os.unlink(self.path)


def save_raw_snapshot(tmp_path: str, root: Path, device: str, verb: str) -> str:
    """Copy stdout as it came, for a verb whose rule says that's safe."""
    target, file = _open_snapshot(root, device, verb)
    try:
        with file, open(tmp_path, encoding="utf-8", errors="replace") as source:
            for line in source:
                file.write(line)
    except OSError:
        os.unlink(target)
        raise
    return str(target)


class Job:
    """One running command. stop() can arrive from another thread at any point, even before it starts."""

    def __init__(self, kill_after: float = KILL_AFTER_SECONDS):
        # How long stop() waits after SIGTERM before SIGKILL: a tool that finishes up when
        # interrupted (Parkbench flushes to Parquet) needs longer than one that just exits.
        self.kill_after = kill_after
        self._lock = threading.Lock()
        self._proc: subprocess.Popen | None = None
        self.stopped = False
        self.secrets: queue.Queue | None = None  # A login that's waiting for its secret.

    def spawn(self, argv: list[str], stdout, stderr, stdin=subprocess.DEVNULL, preexec_fn=None, cwd=None) -> subprocess.Popen:
        proc = subprocess.Popen(argv, stdin=stdin, stdout=stdout, stderr=stderr, shell=False, preexec_fn=preexec_fn, cwd=cwd,
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
                proc.wait(self.kill_after)
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

        rule = verb.snapshot
        trimmed = TrimmedSnapshot(rule, snapshot_root, table.device, verb.name) if rule and not rule.raw else None
        rows: deque = deque(maxlen=table.max_rows)
        count = bad_lines = 0
        def add(row) -> None:
            nonlocal count
            rows.append(row)
            count += 1
            if trimmed:
                trimmed.add(row)

        if verb.output == "document":
            try:
                document = json.loads(Path(tmp_path).read_bytes() or b"[]")
            except json.JSONDecodeError:
                bad_lines = 1
            else:
                for row in document if isinstance(document, list) else [document]:
                    add(row)
        else:
            with open(tmp_path, "rb") as lines:
                for line in lines:
                    if not line.strip():
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        bad_lines += 1  # Never saved by a trimmed snapshot: there's no telling what it holds.
                        continue
                    add(row)

        result = {"ok": True, "rows": list(rows), "count": count, "truncated": count > len(rows),
                  "bad_lines": bad_lines, "snapshot": None}
        if trimmed:
            result["snapshot"] = trimmed.finish()
            if trimmed.error:
                result["snapshot_error"] = trimmed.error
        elif rule:
            try:
                result["snapshot"] = save_raw_snapshot(tmp_path, snapshot_root, table.device, verb.name)
            except OSError as e:
                # The data is good. A snapshot that can't be saved shouldn't cost the face its result.
                result["snapshot_error"] = str(e)
        return result
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def run_stream(table: Table, verb: Verb, binary: str, argv: list[str], job: Job, on_line, snapshot_root: Path) -> dict:
    """Run until the tool exits or stop arrives, sending each line as it comes. The face gets each
    line whole; a verb's rule says what of it is saved. A stream that sent nothing leaves no file."""
    trimmed = TrimmedSnapshot(verb.snapshot, snapshot_root, table.device, verb.name) if verb.snapshot else None
    with tempfile.TemporaryFile() as stderr:
        proc = job.spawn([binary, *argv], subprocess.PIPE, stderr)
        lines = 0
        for raw in proc.stdout:
            if not raw.strip():
                continue
            lines += 1
            text = raw.decode("utf-8", "replace").rstrip("\r\n")
            try:
                row = json.loads(text)
            except json.JSONDecodeError:
                on_line({"text": text})
                continue
            if trimmed:
                trimmed.add(row)
            on_line({"line": row})
        proc.stdout.close()
        exit_code = proc.wait()
        saved = {}
        if trimmed:
            saved["snapshot"] = trimmed.finish(keep_empty=False)
            if trimmed.error:
                saved["snapshot_error"] = trimmed.error
        if job.stopped:
            return {"ok": True, "lines": lines, "stopped": True, **saved}
        if exit_code != 0:
            return {**_failure(table, stderr, exit_code), "lines": lines, "stopped": False, **saved}
        return {"ok": True, "lines": lines, "stopped": False, **saved}


def run_login(verb: Verb, binary: str, argv: list[str], job: Job, on_waiting) -> dict:
    """Run a login that insists on a terminal, and give it the secret when the person has it.

    A tool's terminal output can echo the secret back, in pieces. So everything it prints is read
    and thrown away, unlooked at, and the result comes only from its exit code. The secret goes
    to the terminal and nowhere else: not an argument, a log, a snapshot or a message.
    """
    job.secrets = queue.Queue(maxsize=1)
    master, slave = pty.openpty()
    try:
        attrs = termios.tcgetattr(slave)
        attrs[3] &= ~termios.ECHO
        termios.tcsetattr(slave, termios.TCSANOW, attrs)
        # Its own session, with this terminal as its controlling one, as a real terminal would be.
        proc = job.spawn([binary, *argv], slave, slave, stdin=slave,
                         preexec_fn=lambda: fcntl.ioctl(slave, termios.TIOCSCTTY, 0))
    except BaseException:
        os.close(master)
        os.close(slave)
        raise
    os.close(slave)
    on_waiting()
    deadline = time.monotonic() + verb.timeout
    timed_out = sent = False
    try:
        while True:
            if not sent:
                try:
                    secret = job.secrets.get_nowait()
                except queue.Empty:
                    pass
                else:
                    os.write(master, secret.encode() + b"\n")
                    del secret
                    sent = True
            if time.monotonic() > deadline:
                timed_out = True
                job.stop()
                break
            ready, _, _ = select.select([master], [], [], LOGIN_POLL_SECONDS)
            if ready:
                try:
                    if not os.read(master, 4096):  # Thrown away on purpose.
                        break
                except OSError:  # The tool closed its end.
                    break
    finally:
        job.secrets = None
        os.close(master)
    try:
        exit_code = proc.wait(LOGIN_EXIT_WAIT_SECONDS)
    except subprocess.TimeoutExpired:
        job.stop()
        exit_code = proc.wait()
    if job.stopped and not timed_out:
        return {"ok": False, "stopped": True, "error": "stopped", "code": "stopped", "exit": exit_code}
    if timed_out:
        return {"ok": False, "error": "The login timed out. Press LOGIN to start again.", "code": "timeout", "exit": exit_code}
    if exit_code == 0 and sent:
        return {"ok": True, "exit": 0}
    if not sent:
        return {"ok": False, "error": "flowctl stopped before it had a token.", "code": "login_failed", "exit": exit_code}
    return {"ok": False, "error": LOGIN_REFUSED, "code": "login_failed", "exit": exit_code}


def tool_version(table: Table, binary: str) -> str | None:
    """The tool's version, or None if it can't say."""
    try:
        done = subprocess.run([binary, *table.version_argv], stdin=subprocess.DEVNULL, capture_output=True,
                              text=True, timeout=15)
    except (OSError, subprocess.TimeoutExpired):
        return None
    match = table.version_pattern.search(done.stdout + done.stderr)
    return match.group(1) if match else None
