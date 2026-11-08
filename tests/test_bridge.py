"""The bridge against a fake tool. No test needs flowctl or a network."""

import io
import json
import os
import queue
import stat
import threading
import time

import pytest

from cli_bridge.serve import Server, tool_report
from cli_bridge.table import BadRequest, build_argv, load_table
from cli_bridge.tools import flowctl

FAKE_TOOL = r"""#!/usr/bin/env python3
import json, os, signal, sys, time
args = sys.argv[1:]
with open(os.environ["FAKE_CALLS"], "a") as f:
    f.write(json.dumps(args) + "\n")
if args == ["--version"]:
    print("fake " + os.environ.get("FAKE_VERSION", "1.2.3")); sys.exit(0)
if args and args[0] in ("--profile", "-P"):
    args = args[2:]
cmd = args[0]
if cmd == "rows":
    sys.stderr.write("\x1b[33m WARN fake: please update\x1b[0m\n")
    print('{"n": 1}'); print("not json"); print('{"n": 2}')
elif cmd == "many":
    for n in range(5): print(json.dumps({"n": n}))
elif cmd == "empty":
    pass
elif cmd == "denied":
    sys.stderr.write("\x1b[33m WARN fake: please update\x1b[0m\n\x1b[31mtrace noise\x1b[0m\n")
    sys.stderr.write("Error: fetching\n\nCaused by:\n    no valid login here\n"); sys.exit(1)
elif cmd == "slow":
    time.sleep(30)
elif cmd == "follow":
    print('{"line": 1}', flush=True); print("plain text", flush=True)
    if "--then-exit" in args: sys.stderr.write("Error: lost it\n"); sys.exit(3)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    time.sleep(30)
"""

TABLE = {
    "device": "fake1",
    "tool": "fake",
    "binary": "fake-tool",
    "binary_env": "FAKE_TOOL_PATH",
    "version_argv": ["--version"],
    "version_pattern": r"(\d+\.\d+\.\d+)",
    "tested_versions": ["1.2.3"],
    "profile_flag": "-P",
    "profile_type": "word",
    "max_rows": 3,
    "stderr_noise": "WARN fake",
    "error_codes": [{"contains": "no valid login", "code": "not_logged_in"}],
    "types": {"word": "[a-z0-9_/]+"},
    "verbs": {
        "rows": {"argv": ["rows"], "mode": "once", "args": {"name": {"type": "word", "flag": "--name"}}},
        "many": {"argv": ["many"], "mode": "once"},
        "empty": {"argv": ["empty"], "mode": "once"},
        "denied": {"argv": ["denied"], "mode": "once"},
        "slow": {"argv": ["slow"], "mode": "once"},
        "short": {"argv": ["slow"], "mode": "once", "timeout": 0.2},
        "private": {"argv": ["rows"], "mode": "once", "snapshot": False},
        "follow": {"argv": ["follow"], "mode": "stream", "snapshot": False},
        "follow_ends": {"argv": ["follow", "--then-exit"], "mode": "stream", "snapshot": False},
    },
}


class Client:
    """Talks to a Server over real pipes, the way an app does."""

    def __init__(self, table, snapshots):
        self.input_r, self.input_w = os.pipe()
        self.stdin = os.fdopen(self.input_r, "r")
        self.requests = os.fdopen(self.input_w, "w")
        self.messages: queue.Queue = queue.Queue()
        client = self

        class Out(io.StringIO):
            def write(self, text):
                for line in text.splitlines():
                    client.messages.put(json.loads(line))
                return len(text)

        self.server = Server(table, self.stdin, Out(), snapshot_root=snapshots)
        self.thread = threading.Thread(target=self.server.serve, daemon=True)
        self.thread.start()

    def send(self, **request):
        self.requests.write(json.dumps(request) + "\n")
        self.requests.flush()

    def next(self, timeout=10):
        return self.messages.get(timeout=timeout)

    def until(self, rid, kind, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            m = self.next(deadline - time.time())
            if m.get("id") == rid and m["type"] == kind:
                return m
        raise AssertionError(f"no {kind} for {rid}")

    def result(self, **request):
        self.send(**request)
        return self.until(request["id"], "result")["result"]

    def close(self):
        self.requests.close()
        self.thread.join(10)


@pytest.fixture
def env(tmp_path, monkeypatch):
    tool = tmp_path / "fake-tool"
    tool.write_text(FAKE_TOOL)
    tool.chmod(0o755)
    calls = tmp_path / "calls.jsonl"
    monkeypatch.setenv("FAKE_TOOL_PATH", str(tool))
    monkeypatch.setenv("FAKE_CALLS", str(calls))
    path = tmp_path / "table.json"
    path.write_text(json.dumps(TABLE))
    return {"table": load_table(path), "snapshots": tmp_path / "Snapshots", "calls": calls, "tool": tool}


@pytest.fixture
def client(env):
    c = Client(env["table"], env["snapshots"])
    yield c
    c.close()


def calls(env):
    if not env["calls"].exists():
        return []
    return [json.loads(line) for line in env["calls"].read_text().splitlines()]


# hello


def test_hello_reports_bridge_tool_and_verbs(client):
    client.send(id="h", type="hello")
    m = client.until("h", "hello")
    assert m["protocol"] == 1 and m["device"] == "fake1"
    assert m["tool"]["version"] == "1.2.3" and m["tool"]["status"] == "tested" and m["tool"]["warning"] is None
    assert m["verbs"]["rows"]["args"]["name"]["type"] == "word"


def test_hello_warns_on_an_untested_version(env, client, monkeypatch):
    monkeypatch.setenv("FAKE_VERSION", "9.9.9")
    client.send(id="h", type="hello")
    tool = client.until("h", "hello")["tool"]
    assert tool["status"] == "untested" and "9.9.9" in tool["warning"] and "1.2.3" in tool["warning"]


def test_a_missing_tool_still_says_hello_and_verbs_explain(env, client, monkeypatch):
    monkeypatch.setenv("FAKE_TOOL_PATH", "/nowhere/fake-tool")
    client.send(id="h", type="hello")
    assert client.until("h", "hello")["tool"]["status"] == "missing"
    client.send(id="r", type="rows")
    m = client.until("r", "error")
    assert m["code"] == "tool_missing" and "FAKE_TOOL_PATH" in m["error"]


# Only what the table lists


@pytest.mark.parametrize("kind", ["publish", "delete", "draft", "catalog", "rm -rf /", "", None])
def test_unlisted_types_never_run_anything(env, client, kind):
    client.send(id="r", type=kind)
    assert client.until("r", "error")["code"] == "bad_request"
    assert calls(env) == []


@pytest.mark.parametrize("args", [
    {"name": "--dangerous-auto-approve"},  # a value can't become a flag
    {"name": "-rf"},
    {"name": "a b"},
    {"name": "a;rm"},
    {"name": "$(whoami)"},
    {"name": "a\nb"},
    {"name": ""},
    {"name": 7},
    {"name": ["x"]},
    {"name": "x" * 600},
    {"other": "x"},  # not an argument of this verb
    "name",
])
def test_bad_arguments_never_run_anything(env, client, args):
    client.send(id="r", type="rows", args=args)
    assert client.until("r", "error")["code"] == "bad_request"
    assert calls(env) == []


def test_a_bad_profile_never_runs_anything(env, client):
    client.send(id="r", type="rows", profile="--evil")
    client.until("r", "error")
    assert calls(env) == []


def test_argv_comes_from_the_table_with_the_tools_own_profile_flag(env):
    verb = env["table"].verbs["rows"]
    assert build_argv(env["table"], verb, {"name": "abc"}, "me") == ["-P", "me", "rows", "--name", "abc"]


def test_required_defaults_and_many():
    t = flowctl.table()
    assert build_argv(t, t.verbs["raw_stats"], {"task": "acme/x"}) == [
        "raw", "stats", "-o", "json", "--task", "acme/x", "--since", "24h"]
    assert build_argv(t, t.verbs["catalog_status"], {"names": ["acme/x", "acme/y"]}) == [
        "catalog", "status", "-o", "json", "acme/x", "acme/y"]
    for bad in ({}, {"names": []}, {"names": "acme/x"}, {"names": ["-x/y"]}):
        with pytest.raises(BadRequest):
            build_argv(t, t.verbs["catalog_status"], bad)


# once


def test_once_returns_rows_and_saves_the_raw_output(env, client):
    r = client.result(id="r", type="rows", profile="me")
    assert r["ok"] and r["rows"] == [{"n": 1}, {"n": 2}] and r["bad_lines"] == 1
    assert calls(env)[-1] == ["-P", "me", "rows"]
    path = r["snapshot"]
    assert path.startswith(str(env["snapshots"] / "fake1")) and path.endswith("_rows.jsonl")
    assert open(path).read() == '{"n": 1}\nnot json\n{"n": 2}\n'  # raw, not cleaned up


def test_snapshots_are_private(env, client):
    r = client.result(id="r", type="rows")
    assert stat.S_IMODE(os.stat(r["snapshot"]).st_mode) == 0o600
    assert stat.S_IMODE(os.stat(env["snapshots"] / "fake1").st_mode) == 0o700


def test_same_verb_in_the_same_second_keeps_both_snapshots(client):
    assert client.result(id="a", type="rows")["snapshot"] != client.result(id="b", type="rows")["snapshot"]


def test_a_verb_can_opt_out_of_snapshots(env, client):
    r = client.result(id="r", type="private")
    assert r["ok"] and r["snapshot"] is None and not env["snapshots"].exists()


def test_empty_output_is_an_empty_result(client):
    r = client.result(id="r", type="empty")
    assert r["ok"] and r["rows"] == [] and r["count"] == 0 and os.path.getsize(r["snapshot"]) == 0


def test_the_face_gets_the_newest_rows_and_the_snapshot_has_them_all(client):
    r = client.result(id="r", type="many")
    assert r["truncated"] and r["count"] == 5 and [x["n"] for x in r["rows"]] == [2, 3, 4]
    assert sum(1 for _ in open(r["snapshot"])) == 5


def test_a_failure_gets_a_clean_message_a_code_and_no_snapshot(env, client):
    r = client.result(id="r", type="denied")
    assert not r["ok"] and r["exit"] == 1 and r["code"] == "not_logged_in"
    assert r["error"].startswith("Error: fetching") and "no valid login here" in r["error"]
    assert "\x1b" not in r["error"] and "WARN" not in r["error"] and "trace noise" not in r["error"]
    assert not env["snapshots"].exists()


def test_a_command_that_hangs_times_out(env, client):
    r = client.result(id="r", type="short")
    assert not r["ok"] and r["code"] == "timeout"
    assert not env["snapshots"].exists()


def test_stop_ends_a_once_request_without_a_snapshot(env, client):
    client.send(id="r", type="slow")
    client.until("r", "accepted")
    client.send(id="s", type="stop", request="r")
    client.until("s", "ok")
    r = client.until("r", "result")["result"]
    assert r["stopped"] and not r["ok"]
    assert not env["snapshots"].exists()


def test_stop_needs_a_request_and_tolerates_a_finished_one(client):
    client.send(id="s", type="stop")
    client.until("s", "error")
    client.send(id="t", type="stop", request="long-gone")
    client.until("t", "ok")


def test_a_running_id_cant_be_reused(client):
    client.send(id="r", type="slow")
    client.until("r", "accepted")
    client.send(id="r", type="rows")
    client.until("r", "error")
    client.send(id="s", type="stop", request="r")
    client.until("r", "result")


# stream


def test_stream_sends_lines_until_stopped(env, client):
    client.send(id="r", type="follow")
    client.until("r", "accepted")
    assert client.until("r", "event")["line"] == {"line": 1}
    assert client.until("r", "event")["text"] == "plain text"
    client.send(id="s", type="stop", request="r")
    r = client.until("r", "result")["result"]
    assert r == {"ok": True, "lines": 2, "stopped": True}
    assert not env["snapshots"].exists()


def test_a_stream_that_dies_says_why(client):
    client.send(id="r", type="follow_ends")
    r = client.until("r", "result")["result"]
    assert not r["ok"] and r["exit"] == 3 and r["lines"] == 2 and "lost it" in r["error"]


# Stopping the engine


def process_gone(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


def running_children(server):
    return [job._proc for job, _ in server._jobs.values() if job._proc]


@pytest.mark.parametrize("how", ["shutdown", "end of input"])
def test_stopping_the_engine_stops_what_it_started(env, how):
    c = Client(env["table"], env["snapshots"])
    c.send(id="r", type="follow")
    c.until("r", "event")
    procs = running_children(c.server)
    assert procs
    if how == "shutdown":
        c.send(id="x", type="shutdown")
        c.until("x", "ok")
    else:
        c.requests.close()
    c.thread.join(10)
    assert not c.thread.is_alive()
    assert procs[0].poll() is not None
    if how == "shutdown":
        c.requests.close()


def test_garbage_and_missing_ids_get_errors(client):
    client.requests.write("not json\n[1, 2]\n")
    client.requests.flush()
    assert client.next()["type"] == "error"
    assert client.next()["type"] == "error"
    client.send(type="hello")
    assert client.next()["type"] == "error"


# flowctl's table


def test_flowctl_table_is_read_only_for_now():
    """This PR's scope. Login, drafts and publish arrive in their own PRs and change this test on purpose."""
    t = flowctl.table()
    assert set(t.verbs) == {"catalog_list", "catalog_status", "catalog_history", "raw_stats", "raw_list_shards",
                            "logs_follow"}
    words = {word for verb in t.verbs.values() for word in verb.argv}
    words |= {a.flag for verb in t.verbs.values() for a in verb.args}
    for banned in ("publish", "delete", "draft", "auth", "discover", "preview", "update", "rpc", "--models",
                   "--auto-approve", "--dangerous-auto-approve"):
        assert banned not in words


def test_flowctl_log_follows_are_not_snapshotted():
    assert flowctl.table().verbs["logs_follow"].snapshot is False


@pytest.mark.parametrize("name", ["Early_Signal/jev-datagen/cart_updates", "acme/a", "a.b/c-d_e", "Ünï/tèst"])
def test_flowctl_accepts_real_names(name):
    t = flowctl.table()
    assert build_argv(t, t.verbs["catalog_history"], {"name": name})[-1] == name


@pytest.mark.parametrize("name", ["acme", "/acme/a", "acme/", "acme//a", "acme/../b", "-x/y", "a b/c", "a/b;c", "a/b\n"])
def test_flowctl_rejects_bad_names(name):
    t = flowctl.table()
    with pytest.raises(BadRequest):
        build_argv(t, t.verbs["catalog_history"], {"name": name})


@pytest.mark.parametrize("since,ok", [("1h", True), ("1d2h3m", True), ("90s", True), ("1", False), ("h", False),
                                      ("1 h", False), ("-1h", False)])
def test_flowctl_durations(since, ok):
    t = flowctl.table()
    if ok:
        assert build_argv(t, t.verbs["logs_follow"], {"task": "a/b", "since": since})[-1] == since
    else:
        with pytest.raises(BadRequest):
            build_argv(t, t.verbs["logs_follow"], {"task": "a/b", "since": since})


def test_flowctl_prefixes_need_a_trailing_slash():
    t = flowctl.table()
    assert build_argv(t, t.verbs["catalog_list"], {"prefix": "Early_Signal/"})[-1] == "Early_Signal/"
    assert build_argv(t, t.verbs["catalog_list"])[-1] == "json"  # no prefix: everything
    with pytest.raises(BadRequest):
        build_argv(t, t.verbs["catalog_list"], {"prefix": "Early_Signal"})


def test_tool_report_handles_a_tool_that_wont_say_its_version(env, monkeypatch):
    env["tool"].write_text("#!/bin/sh\nexit 1\n")
    report = tool_report(env["table"])
    assert report["status"] == "untested" and report["version"] is None and "unknown version" in report["warning"]
