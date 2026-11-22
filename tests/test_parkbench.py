"""PB-1's engine (Parkbench's table) against a fake parkbench. No test needs Parkbench or a database."""

import io
import json
import os
import queue
import threading
import time

import pytest

from cli_bridge.serve import Server, tool_report
from cli_bridge.table import BadRequest, build_argv
from cli_bridge.tools import parkbench
from cli_bridge.tools.parkbench.quack import QuackServer

PASSWORD = "pw-7f3k-not-real-qz9"

FAKE_PARKBENCH = r"""#!/usr/bin/env python3
import json, os, signal, sys, time
args = sys.argv[1:]
with open(os.environ["FAKE_CALLS"], "a") as f:
    f.write(json.dumps({"args": args, "cwd": os.getcwd(), "token": os.environ.get("PARKBENCH_QUACK_TOKEN")}) + "\n")
if args[:1] == ["quack-serve"]:
    if os.environ.get("FAKE_QUACK_FAIL"):
        print("Error: start quack server: address in use", file=sys.stderr)
        sys.exit(1)
    signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
    print(json.dumps({"event": "serving", "uri": args[args.index("--quack-uri") + 1]}), flush=True)
    while True:
        time.sleep(0.05)
if args[:1] == ["--version"]:
    print("Error: unknown flag: --version", file=sys.stderr)
    sys.exit(1)
if args[:2] == ["secrets", "create"]:
    values = json.loads(sys.stdin.read())
    with open(os.environ["FAKE_STDIN"], "w") as f:
        json.dump(values, f)
    if values.get("host") == "refuse":
        # A tool that, against its promise, quotes a value back.
        print("Error: create postgres secret: bad value " + values.get("password", ""), file=sys.stderr)
        sys.exit(1)
    print("created")
    sys.exit(0)
if args[:2] == ["secrets", "list"]:
    print(json.dumps({"name": "ducklake_prod", "type": "ducklake", "provider": "config", "persistent": True,
                      "storage": "local_file", "scope": [], "secret_string": "should never be kept"}))
    sys.exit(0)
if args[:1] in (["setup"], ["reset"], ["secrets"]):
    print("✓ done (human text, not JSON)")
    sys.exit(0)
if args[:1] == ["run"]:
    stopped = []
    signal.signal(signal.SIGTERM, lambda *_: stopped.append(1))
    forever = "--num-batches" in args and args[args.index("--num-batches") + 1] == "0"
    print("banner on stderr", file=sys.stderr)
    n = 0
    while forever or n < 3:
        if stopped:
            break
        n += 1
        if n == 2:
            print(json.dumps({"event": "flush", "batch": 2, "secs": 0.1}), flush=True)
        print(json.dumps({"event": "batch", "batch": n, "num_batches": 3, "rows": 10, "batch_secs": 0.01,
                          "rows_total": n * 10, "elapsed_secs": n, "rows_per_sec": 10.0, "table_rows": n * 10}), flush=True)
        time.sleep(0.05)
    print(json.dumps({"event": "done", "reason": "interrupted" if stopped else "complete", "steps": n,
                      "rows_total": n * 10, "elapsed_secs": n, "rows_per_sec": 10.0, "duplicates": 0, "drifts": 0}), flush=True)
    sys.exit(0)
print("Error: unknown command", file=sys.stderr)
sys.exit(1)
"""


class Client:
    def __init__(self, server_kwargs, quack_folder):
        r, w = os.pipe()
        self.requests = os.fdopen(w, "w")
        self.messages: queue.Queue = queue.Queue()
        client = self

        class Out(io.StringIO):
            def write(self, text):
                for line in text.splitlines():
                    client.messages.put(json.loads(line))
                return len(text)

        self.quack = QuackServer(quack_folder, parkbench.table().find_binary, port=19495, token="tok-test-123")
        self.server = Server(parkbench.table(), os.fdopen(r, "r"), Out(), handlers=parkbench.handlers(self.quack),
                             prepare=self.quack.prepare, **server_kwargs)
        self.thread = threading.Thread(target=self.server.serve, daemon=True)
        self.thread.start()

    def send(self, **request):
        self.requests.write(json.dumps(request) + "\n")
        self.requests.flush()

    def until(self, rid, kind, timeout=10):
        deadline = time.time() + timeout
        seen = []
        while time.time() < deadline:
            m = self.messages.get(timeout=max(0.01, deadline - time.time()))
            if m.get("id") == rid:
                seen.append(m)
                if m["type"] == kind:
                    return m, seen
        raise AssertionError(f"no {kind} for {rid}")

    def result(self, **request):
        self.send(**request)
        return self.until(request["id"], "result")[0]["result"]

    def close(self):
        self.requests.close()
        self.thread.join(10)
        self.quack.stop()


@pytest.fixture
def env(tmp_path, monkeypatch):
    tool = tmp_path / "parkbench"
    tool.write_text(FAKE_PARKBENCH)
    tool.chmod(0o755)
    monkeypatch.setenv("PB1_PARKBENCH", str(tool))
    monkeypatch.setenv("FAKE_CALLS", str(tmp_path / "calls.jsonl"))
    monkeypatch.setenv("FAKE_STDIN", str(tmp_path / "stdin.json"))
    return tmp_path


@pytest.fixture
def client(env):
    (env / "data").mkdir()
    c = Client({"snapshot_root": env / "Snapshots", "work_root": env / "Work"}, env / "data")
    yield c
    c.close()


def calls(env):
    path = env / "calls.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def argv(verb, args=None):
    table = parkbench.table()
    return build_argv(table, table.verbs[verb], args)


# the table


def test_the_table_loads_with_every_verb():
    assert set(parkbench.table().verbs) == {"setup", "reset", "run", "secrets_list", "secrets_create", "secrets_drop", "quack_start"}


def test_run_asks_for_json_lines_and_maps_pb1s_flags():
    assert argv("run", {"run_mode": "ticker", "metadata_store": "duckdb", "tick_interval": "0.25", "duration": "300",
                        "duplicate_rate": "0.15", "schema_drift_rate": "0.2", "flush_interval": "8"}) == [
        "run", "--output", "jsonl", "--metadata-store", "duckdb", "--run-mode", "ticker", "--flush-interval", "8",
        "--tick-interval", "0.25", "--duration", "300", "--duplicate-rate", "0.15", "--schema-drift-rate", "0.2"]


def test_a_switch_is_one_word_so_its_value_cant_be_read_as_an_argument():
    assert argv("run", {"run_mode": "batch", "migrate": "true", "ml_labels": "false"}) == [
        "run", "--output", "jsonl", "--migrate=true", "--run-mode", "batch", "--ml-labels=false"]


def test_reset_never_prompts_because_the_host_asks_first():
    assert argv("reset", {"ducklake_secret": "ducklake_prod"}) == ["reset", "--force", "--ducklake-secret", "ducklake_prod"]


@pytest.mark.parametrize("partition", ["day(ts)", "year(ts), month(ts)", "user_id", "bucket(8, user_id)", "hour(ts),day(ts)"])
def test_pb1s_partitions_are_allowed(partition):
    assert argv("run", {"run_mode": "batch", "partition_by": partition})[-1] == partition


@pytest.mark.parametrize("verb,args", [
    ("run", {}),  # run_mode is required
    ("run", {"run_mode": "batch", "pg_dsn": "host=x password=y"}),  # connection strings are never taken
    ("run", {"run_mode": "batch", "data_path": "/tmp"}),
    ("run", {"run_mode": "forever"}),
    ("run", {"run_mode": "batch", "duplicate_rate": "1.5"}),
    ("run", {"run_mode": "batch", "batch_size": "-1"}),
    ("run", {"run_mode": "batch", "migrate": "yes"}),
    ("run", {"run_mode": "batch", "partition_by": "day(ts)); DROP TABLE x"}),
    ("run", {"run_mode": "batch", "ducklake_secret": "a b"}),
    ("setup", {"metadata_store": "sqlite"}),
    ("secrets_drop", {}),
    ("secrets_drop", {"name": "x;y"}),
    ("secrets_create", {"type": "mysql", "name": "x"}),
])
def test_bad_arguments_are_refused(verb, args):
    with pytest.raises(BadRequest):
        argv(verb, args)


def test_hello_explains_parkbench_has_no_version(env):
    report = tool_report(parkbench.table())
    assert report["status"] == "untested"
    assert "No parkbench version has been marked as tested" in report["warning"]


# running


def test_run_streams_each_event_and_snapshots_only_the_numbers(env, client):
    client.send(id="r", type="run", args={"run_mode": "batch", "num_batches": "3"})
    result, seen = client.until("r", "result")
    events = [m["line"] for m in seen if m["type"] == "event"]
    assert [e["event"] for e in events] == ["batch", "flush", "batch", "batch", "done"]
    assert result["result"]["ok"] is True and result["result"]["stopped"] is False
    snapshot = [json.loads(line) for line in open(result["result"]["snapshot"])]
    assert snapshot[-1] == {"event": "done", "reason": "complete", "steps": 3, "rows_total": 30, "elapsed_secs": 3,
                            "rows_per_sec": 10.0, "duplicates": 0, "drifts": 0}


def test_stop_interrupts_a_run_like_ctrl_c_and_it_still_says_done(env, client):
    client.send(id="r", type="run", args={"run_mode": "batch", "num_batches": "0"})
    client.until("r", "event")
    client.send(id="s", type="stop", request="r")
    result, seen = client.until("r", "result")
    assert result["result"]["stopped"] is True
    assert [m["line"]["event"] for m in seen if m["type"] == "event"][-1] == "done"
    assert seen[-2]["line"]["reason"] == "interrupted"


def test_parkbench_runs_in_the_engines_own_folder(env, monkeypatch):
    data = env / "Data" / "pb1"
    monkeypatch.setenv("PB1_DATA_DIR", str(data))
    started = os.getcwd()
    monkeypatch.setattr(parkbench, "serve_table", lambda table, handlers, prepare: None)
    try:
        parkbench.main()
        assert os.getcwd() == str(data.resolve())
        assert oct(data.stat().st_mode & 0o777) == "0o700"
    finally:
        os.chdir(started)


def test_setup_and_reset_run_and_save_nothing(env, client):
    assert client.result(id="a", type="setup", args={"metadata_store": "duckdb"})["ok"] is True
    assert client.result(id="b", type="reset", args={"metadata_store": "duckdb"})["ok"] is True
    assert [c["args"] for c in calls(env)] == [["setup", "--metadata-store", "duckdb"], ["reset", "--force", "--metadata-store", "duckdb"]]
    assert not (env / "Snapshots").exists()


# secrets


def test_list_keeps_names_and_types_but_never_a_secrets_string(env, client):
    result = client.result(id="l", type="secrets_list")
    assert result["rows"][0]["name"] == "ducklake_prod"
    saved = open(result["snapshot"]).read()
    assert "secret_string" not in saved and "should never be kept" not in saved


def test_create_sends_the_values_on_stdin_never_as_arguments(env, client):
    values = {"host": "db.example.com", "password": PASSWORD}
    result = client.result(id="c", type="secrets_create", args={"type": "postgres", "name": "supabase_pg"}, input={"values": values})
    assert result == {"ok": True, "exit": 0}
    assert json.loads((env / "stdin.json").read_text()) == values
    assert calls(env)[-1]["args"] == ["secrets", "create", "--type", "postgres", "--name", "supabase_pg"]
    assert PASSWORD not in (env / "calls.jsonl").read_text()


def test_a_failed_create_never_repeats_a_value(env, client, capfd):
    result = client.result(id="c", type="secrets_create", args={"type": "postgres", "name": "pg"},
                           input={"values": {"host": "refuse", "password": PASSWORD}})
    assert result["ok"] is False and "withheld" in result["error"]
    assert PASSWORD not in json.dumps(result)
    assert PASSWORD not in "".join(capfd.readouterr())
    assert not (env / "Snapshots").exists()


@pytest.mark.parametrize("bad", [None, {}, {"values": {}}, {"values": {"password": 7}}, {"values": {"pass word": "x"}},
                                 {"values": {"host": "h"}, "extra": 1}, {"values": {"password": "x" * 20_000}}])
def test_a_bad_create_never_runs_and_never_repeats_its_input(env, client, bad):
    client.send(id="c", type="secrets_create", args={"type": "postgres", "name": "pg"}, input=bad)
    error, _ = client.until("c", "error")
    assert "x" * 50 not in error["error"]
    assert calls(env) == []


def test_a_failed_create_keeps_a_message_that_quotes_no_credential(env, client):
    # The host isn't a credential, so a message that names it is passed on whole.
    result = client.result(id="c", type="secrets_create", args={"type": "postgres", "name": "pg"},
                           input={"values": {"host": "refuse", "password": ""}})
    assert result["ok"] is False and result["error"].startswith("Error: create postgres secret")


def test_drop_takes_the_name_as_its_one_argument(env, client):
    assert client.result(id="d", type="secrets_drop", args={"name": "ducklake_stage"})["ok"] is True
    assert calls(env)[-1]["args"] == ["secrets", "drop", "ducklake_stage"]


# the Quack server


def quack_serves(env):
    return [c for c in calls(env) if c["args"][:1] == ["quack-serve"]]


def test_quack_start_serves_the_catalog_once_and_writes_a_private_init_file(env, client):
    first = client.result(id="q1", type="quack_start")
    assert first["ok"] is True and first["uri"] == "quack:localhost:19495"
    init = env / "data" / "query.sql"
    assert first["init"] == str(init)
    assert oct(init.stat().st_mode & 0o777) == "0o600"
    text = init.read_text()
    assert "TOKEN 'tok-test-123'" in text and "ATTACH 'ducklake:quack:localhost:19495' AS wh (DATA_PATH './ducklake_data/')" in text
    assert client.result(id="q2", type="quack_start")["ok"] is True
    serves = quack_serves(env)
    assert len(serves) == 1  # already running: not started again
    assert serves[0]["args"] == ["quack-serve", "--quack-uri", "quack:localhost:19495"]
    assert serves[0]["token"] == "tok-test-123" and "tok-test-123" not in json.dumps(serves[0]["args"])


def test_a_run_on_the_quack_catalog_starts_the_server_first_and_others_dont(env, client, monkeypatch):
    client.quack.environment()
    assert client.result(id="s", type="setup", args={"metadata_store": "duckdb"})["ok"] is True
    assert quack_serves(env) == []
    client.send(id="r", type="run", args={"run_mode": "batch", "metadata_store": "quack", "num_batches": "3"})
    result, _ = client.until("r", "result")
    assert result["result"]["ok"] is True
    order = [c["args"][0] for c in calls(env)]
    assert order[-2:] == ["quack-serve", "run"]
    assert calls(env)[-1]["token"] == "tok-test-123"  # the run attaches with the server's token


def test_stopping_the_server_deletes_the_token_files(env, client):
    client.result(id="q", type="quack_start")
    pid = int((env / "data" / "quack.pid").read_text())
    client.quack.stop()
    assert not (env / "data" / "query.sql").exists() and not (env / "data" / "quack.pid").exists()
    time.sleep(0.2)
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


def test_a_server_left_by_a_crashed_engine_is_stopped_first(env, client):
    client.result(id="q", type="quack_start")
    left = int((env / "data" / "quack.pid").read_text())
    client.quack._proc = None  # as if a new engine started, with the old server still up
    client.result(id="q2", type="quack_start")
    time.sleep(0.2)
    with pytest.raises(ProcessLookupError):
        os.kill(left, 0)
    assert int((env / "data" / "quack.pid").read_text()) != left


def test_a_server_that_cant_start_says_so_with_its_own_code(env, client, monkeypatch):
    monkeypatch.setenv("FAKE_QUACK_FAIL", "1")
    result = client.result(id="q", type="quack_start")
    assert result["ok"] is False and result["code"] == "quack_unavailable" and "port 19495" in result["error"]
    run = client.result(id="r", type="setup", args={"metadata_store": "quack"})
    assert run["code"] == "quack_unavailable"
    assert [c for c in calls(env) if c["args"][:1] == ["setup"]] == []  # setup itself never ran


def test_quack_start_takes_no_input(env, client):
    client.send(id="q", type="quack_start", input={"x": 1})
    error, _ = client.until("q", "error")
    assert "no input" in error["error"]
