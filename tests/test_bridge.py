"""The bridge against a fake tool. No test needs flowctl or a network."""

import io
import json
import os
import queue
import stat
import threading
import time

import pytest

from cli_bridge.run import MAX_SNAPSHOT_TEXT, trim_row
from cli_bridge.serve import Server, tool_report
from cli_bridge.table import BadRequest, SnapshotRule, build_argv, load_table
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
elif cmd == "sensitive":
    print(open(os.environ["FAKE_LINES"]).read(), end="", flush=True)
    if "--follow" in args:
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
        time.sleep(30)
elif cmd == "slow":
    time.sleep(30)
elif cmd == "follow":
    print('{"line": 1}', flush=True); print("plain text", flush=True)
    if "--then-exit" in args: sys.stderr.write("Error: lost it\n"); sys.exit(3)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    time.sleep(30)
"""

RULE = {"keep": ["ts", "level", "shard.name"], "first_line": ["message"], "numbers": ["fields"]}
MARKER = "CUSTOMER-MARKER"

# A real failure's shape (estuary/flow's TestExtractorValidation snapshot), with the document in
# the error as an object and as text, plus what else mustn't get saved.
SENSITIVE_LINES = [
    json.dumps({"ts": "t1", "level": "error", "shard": {"name": "acme/a", "labels": MARKER},
                "message": "document failed validation",
                "fields": {"round": 3, "ok": True, "error": ["failed", {
                    "basic_output": [{"instanceValue": MARKER}], "document": {"email": MARKER}}]}}),
    json.dumps({"ts": "t2", "level": "error", "message": "failed\n\nCaused by:\n    {\"document\": \"" + MARKER + "\"}"}),
    json.dumps({"ts": "t3", "level": "info", "message": "x" * 500, "fields": {"value": MARKER, "nested": {"n": 1}}}),
    f"plain text with {MARKER}",
    json.dumps([MARKER]),
    json.dumps({"other": MARKER}),
]

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
        "rows": {"argv": ["rows"], "mode": "once", "snapshot": {"raw": True},
                 "args": {"name": {"type": "word", "flag": "--name"}}},
        "many": {"argv": ["many"], "mode": "once", "snapshot": {"raw": True}},
        "empty": {"argv": ["empty"], "mode": "once", "snapshot": {"raw": True}},
        "denied": {"argv": ["denied"], "mode": "once", "snapshot": {"raw": True}},
        "slow": {"argv": ["slow"], "mode": "once", "snapshot": {"raw": True}},
        "short": {"argv": ["slow"], "mode": "once", "snapshot": {"raw": True}, "timeout": 0.2},
        "trimmed": {"argv": ["sensitive"], "mode": "once", "snapshot": RULE},
        "trimmed_empty": {"argv": ["empty"], "mode": "once", "snapshot": RULE},
        "trimmed_many": {"argv": ["many"], "mode": "once", "snapshot": {"keep": ["n"]}},
        "trimmed_follow": {"argv": ["sensitive", "--follow"], "mode": "stream", "snapshot": RULE},
        "quiet_follow": {"argv": ["empty"], "mode": "stream", "snapshot": RULE},
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
    lines = tmp_path / "lines.jsonl"
    lines.write_text("\n".join(SENSITIVE_LINES) + "\n")
    monkeypatch.setenv("FAKE_LINES", str(lines))
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


EXPECTED_TRIMMED = [
    {"ts": "t1", "level": "error", "shard": {"name": "acme/a"}, "message": "document failed validation",
     "fields": {"round": 3}},
    {"ts": "t2", "level": "error", "message": "failed"},
    {"ts": "t3", "level": "info", "message": "x" * MAX_SNAPSHOT_TEXT},
]


def saved_text(env):
    return "".join(p.read_text() for p in env["snapshots"].rglob("*.jsonl"))


def test_a_trimmed_snapshot_holds_only_what_the_rule_names(env, client):
    r = client.result(id="r", type="trimmed")
    assert [json.loads(line) for line in open(r["snapshot"])] == EXPECTED_TRIMMED
    assert MARKER not in saved_text(env)
    assert stat.S_IMODE(os.stat(r["snapshot"]).st_mode) == 0o600


def test_the_face_still_gets_every_row_whole(client):
    r = client.result(id="r", type="trimmed")
    assert r["rows"][0]["fields"]["value"] == MARKER and r["rows"][-1] == {"other": MARKER}
    assert r["count"] == 5 and r["bad_lines"] == 1


def test_a_trimmed_snapshot_has_every_row_even_past_the_faces_limit(client):
    r = client.result(id="r", type="trimmed_many")
    assert r["truncated"] and [json.loads(line)["n"] for line in open(r["snapshot"])] == [0, 1, 2, 3, 4]


def test_a_trimmed_empty_result_is_an_empty_file(client):
    assert os.path.getsize(client.result(id="r", type="trimmed_empty")["snapshot"]) == 0


def test_a_stream_saves_trimmed_lines_and_sends_them_whole(env, client):
    client.send(id="r", type="trimmed_follow")
    events = [client.until("r", "event") for _ in SENSITIVE_LINES]
    assert events[0]["line"]["fields"]["error"][1]["document"] == {"email": MARKER}
    assert events[3]["text"] == SENSITIVE_LINES[3]
    client.send(id="s", type="stop", request="r")
    r = client.until("r", "result")["result"]
    assert r["stopped"] and r["lines"] == len(SENSITIVE_LINES)
    assert [json.loads(line) for line in open(r["snapshot"])] == EXPECTED_TRIMMED
    assert MARKER not in saved_text(env)


def test_a_stream_that_sent_nothing_leaves_no_file(env, client):
    r = client.result(id="r", type="quiet_follow")
    assert r["ok"] and r["snapshot"] is None
    assert list(env["snapshots"].rglob("*.jsonl")) == []


def test_a_snapshot_that_cant_be_saved_doesnt_cost_the_result(env, client):
    env["snapshots"].write_text("a file where the folder should be")
    r = client.result(id="r", type="trimmed")
    assert r["ok"] and r["count"] == 5 and r["snapshot"] is None and r["snapshot_error"]
    client.send(id="f", type="trimmed_follow")
    client.until("f", "event")
    client.send(id="s", type="stop", request="f")
    r = client.until("f", "result")["result"]
    assert r["ok"] and r["snapshot"] is None and r["snapshot_error"]


@pytest.mark.parametrize("row,rule,expected", [
    ("text", SnapshotRule(keep=("a",)), None),
    ([1], SnapshotRule(keep=("a",)), None),
    ({"b": 1}, SnapshotRule(keep=("a",)), None),
    ({"a": {"b": 1, "c": 2}}, SnapshotRule(keep=("a.b",)), {"a": {"b": 1}}),
    ({"a": None}, SnapshotRule(keep=("a.b",)), None),
    ({"a": 5}, SnapshotRule(first_line=("a",)), None),
    ({"a": ""}, SnapshotRule(first_line=("a",)), {"a": ""}),
    ({"a": "one\ntwo"}, SnapshotRule(first_line=("a",)), {"a": "one"}),
    ({"a": {"n": 1, "f": 1.5, "t": True, "s": "x", "o": {"n": 1}}}, SnapshotRule(numbers=("a",)), {"a": {"n": 1, "f": 1.5}}),
    ({"a": {"s": "x"}}, SnapshotRule(numbers=("a",)), None),
])
def test_trim_row(row, rule, expected):
    assert trim_row(row, rule) == expected


@pytest.mark.parametrize("snapshot,mode", [
    (True, "once"),
    ("raw", "once"),
    ({}, "once"),
    ({"keep": []}, "once"),
    ({"everything": True}, "once"),
    ({"raw": True, "keep": ["a"]}, "once"),
    ({"raw": True}, "stream"),
])
def test_a_table_cant_load_with_a_bad_snapshot_rule(tmp_path, snapshot, mode):
    path = tmp_path / "bad.json"
    path.write_text(json.dumps({**TABLE, "verbs": {"v": {"argv": ["v"], "mode": mode, "snapshot": snapshot}}}))
    with pytest.raises(ValueError, match="verb v"):
        load_table(path)


def test_a_table_cant_load_with_a_verb_that_doesnt_say_what_it_saves(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text(json.dumps({**TABLE, "verbs": {"v": {"argv": ["v"], "mode": "once"}}}))
    with pytest.raises(ValueError, match="needs a snapshot rule"):
        load_table(path)


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
                            "logs_read", "logs_follow"}
    words = {word for verb in t.verbs.values() for word in verb.argv}
    words |= {a.flag for verb in t.verbs.values() for a in verb.args}
    for banned in ("publish", "delete", "draft", "auth", "discover", "preview", "update", "rpc", "--models",
                   "--auto-approve", "--dangerous-auto-approve"):
        assert banned not in words


def test_flowctl_only_saves_stats_raw():
    assert [name for name, verb in flowctl.table().verbs.items() if verb.snapshot.raw] == ["raw_stats"]


def test_flowctl_logs_never_save_a_failing_document():
    # estuary/flow's real validation failure, as a log line.
    line = {
        "_meta": {"uuid": "u1"}, "ts": "t", "level": "error", "shard": {"name": "acme/a", "kind": "capture", "build": "b"},
        "message": f"source document failed validation\n\nCaused by:\n    {{\"document\": {{\"email\": \"{MARKER}\"}}}}",
        "fields": {"round": 2, "error": ["failed", {"basic_output": [{"instanceValue": MARKER}], "document": {"e": MARKER}}]},
    }
    verbs = flowctl.table().verbs
    assert verbs["logs_follow"].snapshot == verbs["logs_read"].snapshot
    assert trim_row(line, verbs["logs_read"].snapshot) == {
        "_meta": {"uuid": "u1"}, "ts": "t", "level": "error", "shard": {"name": "acme/a", "kind": "capture"},
        "message": "source document failed validation", "fields": {"round": 2},
    }


def test_flowctl_catalog_list_drops_who_published_and_the_spec():
    row = {
        "catalogName": "acme/a", "userCapability": "admin",
        "lastPublication": {"userEmail": MARKER, "userFullName": MARKER},
        "liveSpec": {"catalogType": "capture", "isDisabled": False, "updatedAt": "t", "dataPlaneId": MARKER,
                     "model": {"endpoint": {"connector": {"config": {"address": MARKER, "password_sops": MARKER}}}},
                     "readsFrom": None, "writesTo": {"edges": [{"node": {"catalogName": "acme/a/t"}}]}},
        "status": {"type": "OK", "summary": f"Fine\n{MARKER}"},
    }
    trimmed = trim_row(row, flowctl.table().verbs["catalog_list"].snapshot)
    assert MARKER not in json.dumps(trimmed)
    assert trimmed["liveSpec"]["writesTo"] == row["liveSpec"]["writesTo"] and trimmed["status"]["summary"] == "Fine"
    assert set(trimmed) == {"catalogName", "liveSpec", "status"}


def test_flowctl_history_and_status_drop_people_and_failure_text():
    verbs = flowctl.table().verbs
    history = {"catalog_name": "acme/a", "catalog_type": "capture", "publication": {
        "publicationId": "p", "publishedAt": "t", "detail": MARKER, "userEmail": MARKER, "userFullName": MARKER,
        "userAvatarUrl": MARKER, "userId": MARKER, "model": MARKER}}
    status = {"catalogName": "acme/a", "catalogType": "capture", "status": {"type": "OK", "summary": "ok", "controller": {
        "failures": 1, "error": MARKER,
        "activation": {"recentFailureCount": 1, "lastFailure": {"text": MARKER}},
        "publications": {"history": [{"detail": MARKER}]}}}}
    assert MARKER not in json.dumps(trim_row(history, verbs["catalog_history"].snapshot))
    trimmed = trim_row(status, verbs["catalog_status"].snapshot)
    assert MARKER not in json.dumps(trimmed)
    assert trimmed["status"]["controller"]["activation"]["recentFailureCount"] == 1


def test_flowctl_log_read_has_no_follow():
    t = flowctl.table()
    assert build_argv(t, t.verbs["logs_read"], {"task": "a/b"}) == ["logs", "-o", "json", "--task", "a/b", "--since", "24h"]


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
