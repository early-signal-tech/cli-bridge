"""PB-1's Quack server: one `parkbench quack-serve` that owns the local DuckDB catalog, so runs
and a DuckDB terminal (PB-1's QUERY) can use it at the same time.

DuckDB lets one process open a database file for writing. With the server holding
ducklake_data/wh.ducklake, every request with metadata_store "quack" attaches through it, as does
the terminal, with the init file written here.

The server starts the first time something needs it, not with the engine, and stops when the
engine does. Its token is made fresh for each engine and reaches Parkbench only through
PARKBENCH_QUACK_TOKEN, which every command the engine runs inherits. The init file holds the
token too, so only you can read it, and it's deleted when the server stops. If an engine crashed
and left its server running, the next one stops it first (it finds it by its pid file).
"""

import json
import os
import queue
import secrets
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from ...table import BadRequest

TOKEN_ENV = "PARKBENCH_QUACK_TOKEN"
DEFAULT_PORT = 9495  # Parkbench's --quack-uri default (quack.go); DH-1's traces server uses 9494
# The first start downloads DuckDB's quack extension.
START_TIMEOUT_SECONDS = 60.0
STOP_WAIT_SECONDS = 5.0
PID_FILE = "quack.pid"
INIT_FILE = "query.sql"
# What the catalog stores: Parkbench's --data-path default, as DuckLake writes it down.
DATA_PATH = "./ducklake_data/"


class QuackUnavailable(BadRequest):
    code = "quack_unavailable"


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _is_quack_serve(pid: int) -> bool:
    found = subprocess.run(["ps", "-p", str(pid), "-o", "command="], capture_output=True, text=True).stdout
    return "parkbench" in found and "quack-serve" in found


def _write_private(path: Path, text: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)


class QuackServer:
    def __init__(self, folder: Path, find_binary, port: int = DEFAULT_PORT, token: str | None = None):
        self.folder, self.find_binary, self.port = Path(folder), find_binary, port
        self.token = token or secrets.token_hex(24)
        self.uri = f"quack:localhost:{port}"
        self._lock = threading.Lock()
        self._proc: subprocess.Popen | None = None

    @property
    def init_path(self) -> Path:
        return self.folder / INIT_FILE

    def environment(self) -> None:
        """Hand the token to every command the engine runs from now on."""
        os.environ[TOKEN_ENV] = self.token

    def ensure(self) -> dict:
        """Start the server if it isn't running. What a terminal needs to attach through it."""
        with self._lock:
            if self._proc is None or self._proc.poll() is not None:
                self._start()
            return {"uri": self.uri, "init": str(self.init_path), "folder": str(self.folder)}

    def _start(self) -> None:
        binary = self.find_binary()
        if not binary:
            raise QuackUnavailable("Can't find parkbench, so the local catalog can't be served.")
        self._stop_stale()
        proc = subprocess.Popen(
            [binary, "quack-serve", "--quack-uri", self.uri], cwd=self.folder, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=sys.stderr, start_new_session=True,
            env={**os.environ, TOKEN_ENV: self.token, "NO_COLOR": "1"}, text=True)
        lines: queue.Queue = queue.Queue()
        threading.Thread(target=lambda: [lines.put(line) for line in proc.stdout], daemon=True).start()
        deadline = time.monotonic() + START_TIMEOUT_SECONDS
        while True:
            try:
                line = lines.get(timeout=0.2)
            except queue.Empty:
                if proc.poll() is not None:
                    raise QuackUnavailable(
                        f"parkbench quack-serve stopped (exit {proc.returncode}) before it was serving; "
                        f"is port {self.port} in use? Its message is in the engine's log.")
                if time.monotonic() > deadline:
                    self._kill(proc)
                    raise QuackUnavailable(f"parkbench quack-serve didn't start within {START_TIMEOUT_SECONDS:g} seconds.")
                continue
            try:
                if json.loads(line).get("event") == "serving":
                    break
            except ValueError:
                continue
        self._proc = proc
        (self.folder / PID_FILE).write_text(str(proc.pid))
        _write_private(self.init_path, (
            "-- PB-1's QUERY: DuckDB on PB-1's local catalog, through the engine's Quack server.\n"
            "-- It holds the server's token, so only you can read it; it's deleted when the server stops.\n"
            "LOAD quack;\n"
            f"CREATE OR REPLACE SECRET pb1_quack (TYPE quack, TOKEN '{self.token}');\n"
            f"ATTACH 'ducklake:{self.uri}' AS wh (DATA_PATH '{DATA_PATH}');\n"
            "USE wh;\n"))

    def _stop_stale(self) -> None:
        pid_file = self.folder / PID_FILE
        try:
            pid = int(pid_file.read_text().strip())
        except (OSError, ValueError):
            return
        if _alive(pid) and _is_quack_serve(pid):
            os.killpg(pid, signal.SIGINT)
            deadline = time.monotonic() + STOP_WAIT_SECONDS
            while _alive(pid) and time.monotonic() < deadline:
                time.sleep(0.05)
            if _alive(pid):
                os.killpg(pid, signal.SIGKILL)
        pid_file.unlink(missing_ok=True)

    @staticmethod
    def _kill(proc: subprocess.Popen) -> None:
        try:
            os.killpg(proc.pid, signal.SIGINT)
            proc.wait(STOP_WAIT_SECONDS)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait()

    def stop(self) -> None:
        with self._lock:
            if self._proc is not None and self._proc.poll() is None:
                self._kill(self._proc)
            self._proc = None
            self.init_path.unlink(missing_ok=True)
            (self.folder / PID_FILE).unlink(missing_ok=True)

    # The engine's hooks

    def prepare(self, verb, request: dict) -> None:
        """A setup, run or reset on the Quack catalog needs the server up first."""
        args = request.get("args") if isinstance(request, dict) else None
        if verb.name in ("setup", "run", "reset") and isinstance(args, dict) and args.get("metadata_store") == "quack":
            self.ensure()

    def check(self, payload) -> None:
        if payload is not None:
            raise BadRequest("quack_start takes no input")

    def run(self, table, verb, binary, argv, job, checked, work_root) -> dict:
        try:
            return {"ok": True, **self.ensure()}
        except QuackUnavailable as e:
            return {"ok": False, "error": str(e), "code": e.code, "exit": None}
