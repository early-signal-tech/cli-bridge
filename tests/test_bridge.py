"""The bridge against a fake tool. No test needs flowctl or a network."""

import io
import json
import os
import queue
import stat
import threading
import time

import pytest
from pathlib import Path

from cli_bridge.run import MAX_SNAPSHOT_TEXT, trim_row
from cli_bridge.serve import Handler, Server, tool_report
from cli_bridge.table import BadRequest, SnapshotRule, build_argv, load_table
from cli_bridge.tools import flowctl
from cli_bridge.tools.flowctl import discover as flowctl_discover
from cli_bridge.tools.flowctl import drafts as flowctl_drafts

# Longer than any fragment check below, and nothing like a real token.
FAKE_TOKEN = "zqx7.vkj2-pwm9qlt4-hdn6rcb3-yfg8swa5-jzk1xeu0.mnb4vcz7"

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
elif cmd == "discover":
    spec = json.load(open("flow.yaml"))
    name, capture = next(iter(spec["captures"].items()))
    config = capture["endpoint"]["connector"]["config"]
    secret = config.get("password", "")
    how = config.get("how", "ok")
    with open(os.environ["FAKE_FOLDER"], "a") as f: f.write(os.getcwd() + "\n")
    if how == "fail":
        print("2026 discover>  INFO: connecting\n2026 discover> ERROR: bad password: " + secret)
        sys.stderr.write("Error: failed " + secret + "\n"); sys.exit(1)
    if how == "login":
        sys.stderr.write("Error: no valid login here\n"); sys.exit(1)
    if how == "hang":
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(0)); time.sleep(30)
    print("noise with " + secret)
    if how == "plain":
        pass  # Leaves the config as it was.
    elif how == "unencrypted":
        config = {"address": config.get("address")}
    else:
        config = {"address": config.get("address"), "password_sops": "ENC[AES256_GCM,data:x]", "sops": {"mac": "x"}}
    capture["endpoint"]["connector"]["config"] = config
    capture["bindings"] = [{"resource": {"namespace": "public", "stream": "orders"}, "target": name + "/public/orders"}]
    json.dump(spec, open("flow.yaml", "w"))
elif cmd == "draft":
    state = json.load(open(os.environ["FAKE_DRAFTS"])) if os.path.exists(os.environ["FAKE_DRAFTS"]) else {"drafts": {}, "selected": None}
    sub = args[1]
    if sub == "create":
        state["made"] = state.get("made", 0) + 1
        did = "%016x" % (state["made"] + 0xabc0)
        state["drafts"][did] = None; state["selected"] = did
        print(json.dumps({"id": did, "created_at": "now"}))
    elif sub == "select":
        did = args[args.index("--id") + 1]
        if did not in state["drafts"]: sys.stderr.write("Error: no such draft\n"); sys.exit(1)
        state["selected"] = did
    elif sub == "delete":
        state["drafts"].pop(state["selected"], None); state["selected"] = None
    elif sub == "author":
        catalog = json.load(open(args[args.index("--source") + 1]))
        how = json.dumps(catalog)
        if "author-fails" in how: sys.stderr.write("Error: bad spec near " + how[:200] + "\n"); sys.exit(1)
        if "author-hangs" in how:
            json.dump(state, open(os.environ["FAKE_DRAFTS"], "w"))
            signal.signal(signal.SIGTERM, lambda *_: sys.exit(0)); time.sleep(30)
        if "keep-plain" not in how:
            how = how.replace(os.environ.get("FAKE_DEST_SECRET", "~"), "ENC[x]")
        state["drafts"][state["selected"]] = how
    elif sub == "test":
        spec = state["drafts"].get(state["selected"]) or ""
        with open(os.environ["FAKE_CALLS"], "a") as f: f.write(json.dumps({"tested": state["selected"]}) + "\n")
        if "test-fails" in spec: print("2026 build> ERROR: cannot connect to destination"); sys.stderr.write("Error: test failed\n"); sys.exit(1)
    json.dump(state, open(os.environ["FAKE_DRAFTS"], "w"))
elif cmd == "raw" and args[1] == "get":
    state = json.load(open(os.environ["FAKE_DRAFTS"]))
    did = [q for q in args if q.startswith("draft_id=eq.")][0].split(".", 1)[1]
    stored = state["drafts"].get(did) or "{}"
    print(json.dumps([{"catalog_name": n, "spec_type": k[:-1], "spec": stored} for k, v in json.loads(stored).items() for n in v], indent=1))
elif cmd == "raw" and args[1] == "bundle":
    print(json.dumps(json.load(open("flow.yaml")), indent=2))
elif cmd == "document":
    print(json.dumps([{"n": 1, "big": {"x": 1}}, {"n": 2}], indent=2))
elif cmd == "login":
    with open(os.environ["FAKE_CALLS"], "a") as f:
        f.write(json.dumps({"tty": sys.stdin.isatty()}) + "\n")
    if "--no-prompt" in args: sys.exit(4)
    prompt = open("/dev/tty", "w")  # Fails without a controlling terminal, as a real prompt would.
    prompt.write("Paste your token: "); prompt.flush()
    token = open("/dev/tty").readline().strip()
    # Repeat it in pieces, the way flowctl's prompt redraws do.
    for n in range(0, len(token), 7):
        print("\x1b[2K" + token[:n + 7], flush=True)
        sys.stderr.write(token[n:n + 7] + "\n")
    if token != os.environ["FAKE_GOOD_TOKEN"]:
        sys.stderr.write("Error: JWT token missing payload: " + token + "\n"); sys.exit(1)
    with open(os.environ["FAKE_SAVED"], "w") as f: f.write("saved")
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
        "find": {"argv": ["discover", "--source", "flow.yaml"], "mode": "handler", "snapshot": False, "timeout": 5,
                 "args": {"data_plane": {"type": "word", "flag": "--data-plane"}}},
        "find_quick": {"argv": ["discover", "--source", "flow.yaml"], "mode": "handler", "snapshot": False, "timeout": 0.5},
        "find_nothing": {"argv": ["discover"], "mode": "handler", "snapshot": False},
        "draft_author": {"argv": ["draft", "author"], "mode": "handler", "snapshot": False, "timeout": 5},
        "draft_author_quick": {"argv": ["draft", "author"], "mode": "handler", "snapshot": False, "timeout": 0.5},
        "draft_test": {"argv": ["draft", "test"], "mode": "handler", "snapshot": False, "timeout": 5},
        "draft_delete": {"argv": ["draft", "delete"], "mode": "handler", "snapshot": False, "timeout": 5},
        "document": {"argv": ["document"], "mode": "once", "output": "document", "snapshot": {"keep": ["n"]}},
        "formatted": {"argv": ["rows"], "mode": "once", "snapshot": False,
                      "args": {"name": {"type": "word", "flag": "--q", "format": "x=eq.{}"}}},
        "log_in": {"argv": ["login"], "mode": "login", "snapshot": False},
        "log_in_breaks": {"argv": ["login", "--no-prompt"], "mode": "login", "snapshot": False},
        "log_in_quick": {"argv": ["slow"], "mode": "login", "snapshot": False, "timeout": 0.3},
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

        self.work = snapshots.parent / "Work"
        finder = Handler(flowctl_discover.check, flowctl_discover.run)
        self.server = Server(table, self.stdin, Out(), snapshot_root=snapshots, work_root=self.work,
                             handlers={"find": finder, "find_quick": finder, **{
                                 name: Handler(check, run) for name, check, run in (
                                     ("draft_author", flowctl_drafts.check_author, flowctl_drafts.run_author),
                                     ("draft_author_quick", flowctl_drafts.check_author, flowctl_drafts.run_author),
                                     ("draft_test", flowctl_drafts.check_draft, flowctl_drafts.run_test),
                                     ("draft_delete", flowctl_drafts.check_draft, flowctl_drafts.run_delete))}})
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
    monkeypatch.setenv("FAKE_GOOD_TOKEN", FAKE_TOKEN)
    monkeypatch.setenv("FAKE_SAVED", str(tmp_path / "tool-saved-it"))
    monkeypatch.setenv("FAKE_FOLDER", str(tmp_path / "ran-in"))
    monkeypatch.setenv("FAKE_DRAFTS", str(tmp_path / "drafts.json"))
    monkeypatch.setenv("FAKE_DEST_SECRET", DEST_SECRET)
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


# discover

PASSWORD = "zqx7-vkj2-made-up-pw"


def find_input(how="ok", password=PASSWORD, **extra):
    return {"name": "acme/pg", "image": "ghcr.io/estuary/source-postgres:v3", "secret_paths": ["password"],
            "config": {"address": "db:5432", "password": password, "how": how}, **extra}


def assert_nothing_left(env, client, sent=""):
    """No folder, no snapshot, and no piece of the password in anything sent or saved."""
    assert list(client.work.rglob("*")) == [] or all(p.is_dir() for p in client.work.rglob("*"))
    assert not env["snapshots"].exists()
    for piece in fragments(PASSWORD):
        assert piece not in sent


def test_discover_returns_the_encrypted_catalog_and_deletes_its_folder(env, client):
    r = client.result(id="d", type="find", profile="me", args={"data_plane": "plane"}, input=find_input())
    assert r["ok"] and r["bindings"] == [{"target": "acme/pg/public/orders", "resource": {"namespace": "public", "stream": "orders"}}]
    config = r["catalog"]["captures"]["acme/pg"]["endpoint"]["connector"]["config"]
    assert "password_sops" in config and "sops" in config and "password" not in config
    assert_nothing_left(env, client, json.dumps(r))
    ran_in = (env["tool"].parent / "ran-in").read_text().strip()
    assert ran_in.startswith(str(client.work)) and not os.path.exists(ran_in)
    assert stat.S_IMODE(os.stat(client.work / "fake1").st_mode) == 0o700
    argv = [c for c in calls(env) if "discover" in c][0]
    assert argv == ["-P", "me", "discover", "--source", "flow.yaml", "--data-plane", "plane"]
    assert ["-P", "me", "raw", "bundle", "--source", "flow.yaml", "-o", "json"] in calls(env)


def test_a_secret_is_never_an_argument(env, client):
    client.result(id="d", type="find", input=find_input())
    assert PASSWORD not in json.dumps(calls(env))


@pytest.mark.parametrize("how,code", [("fail", "tool_error"), ("plain", "secret_left"), ("unencrypted", "secret_left")])
def test_no_piece_of_a_secret_gets_out_however_discover_ends(env, tmp_path, capfd, how, code):
    client = Client(env["table"], env["snapshots"])
    client.send(id="d", type="find", input=find_input(how))
    result = client.until("d", "result")["result"]
    client.close()
    assert not result["ok"] and result["code"] == code and "catalog" not in result
    sent = json.dumps(result) + everything_sent(client)
    out, err = capfd.readouterr()
    assert_nothing_left(env, client, sent + out + err)


def test_a_failed_discover_explains_with_the_secret_taken_out(client):
    r = client.result(id="d", type="find", input=find_input("fail"))
    assert r["error"] == "bad password: [hidden]"
    assert r["exit"] == 1


def test_discover_keeps_the_tools_error_codes(client):
    assert client.result(id="d", type="find", input=find_input("login"))["code"] == "not_logged_in"


def test_stop_ends_a_discover_and_deletes_its_folder(env, client):
    client.send(id="d", type="find", input=find_input("hang"))
    client.until("d", "accepted")
    folder = env["tool"].parent / "ran-in"
    deadline = time.time() + 5
    while not folder.exists() and time.time() < deadline:
        time.sleep(0.02)
    client.send(id="s", type="stop", request="d")
    r = client.until("d", "result")["result"]
    assert r["stopped"] and not r["ok"]
    assert_nothing_left(env, client, json.dumps(r))


def test_a_discover_that_hangs_times_out_and_deletes_its_folder(env, client):
    r = client.result(id="d", type="find_quick", input=find_input("hang"))
    assert not r["ok"] and r["code"] == "timeout"
    assert_nothing_left(env, client, json.dumps(r))


def test_an_unexpected_failure_says_nothing(env, monkeypatch):
    def explode(*_):
        raise ValueError(PASSWORD)
    c = Client(env["table"], env["snapshots"])
    c.server.handlers = {"find": Handler(flowctl_discover.check, explode)}
    r = c.result(id="d", type="find", input=find_input())
    c.close()
    assert r["error"] == "find failed unexpectedly." and PASSWORD not in json.dumps(r)


@pytest.mark.parametrize("bad", [
    None, "text", {},
    find_input(name="nope"),
    find_input(name="-x/y"),
    find_input(image="ghcr.io/evil/source-postgres:v3"),
    find_input(image="ghcr.io/estuary/source-postgres:v3 --rm"),
    find_input(image="ghcr.io/estuary/source-http-ingest:v1"),
    find_input(config="text"),
    find_input(secret_paths="password"),
    find_input(secret_paths=[1]),
    find_input(password="x" * 1_100_000),
])
def test_a_bad_discover_never_runs_and_doesnt_repeat_its_input(env, client, bad):
    client.send(id="d", type="find", input=bad)
    m = client.until("d", "error")
    assert m["code"] == "bad_request" and calls(env) == [] and PASSWORD not in json.dumps(m)
    assert not client.work.exists()


def test_a_handler_verb_without_a_handler_is_refused(env, client):
    client.send(id="d", type="find_nothing", input=find_input())
    assert client.until("d", "error")["code"] == "bad_request"
    assert calls(env) == []


def test_a_crashed_runs_folder_is_swept_at_start_and_a_recent_one_isnt(env):
    work = env["snapshots"].parent / "Work" / "fake1"
    old, recent = work / "run-old", work / "run-recent"
    old.mkdir(parents=True)
    recent.mkdir()
    (old / "flow.yaml").write_text(PASSWORD)
    os.utime(old, (time.time() - 7200, time.time() - 7200))
    c = Client(env["table"], env["snapshots"])
    c.send(id="h", type="hello")
    c.until("h", "hello")
    c.close()
    assert not old.exists() and recent.exists()


def test_hide_takes_out_every_form_of_a_secret():
    assert flowctl_discover.hide('a "b" and a \\"b\\"', ['a "b"']) == "[hidden] and [hidden]"
    secret = "line-one-of-key\nline-two-of-key"
    assert flowctl_discover.hide("x line-two-of-key y", [secret]) == "x [hidden] y"
    assert flowctl_discover.hide(json.dumps(secret), [secret]) == '"[hidden]"'


def test_secret_paths_reach_nested_fields_and_skip_what_isnt_there():
    checked = flowctl_discover.check({"name": "a/b", "image": "ghcr.io/estuary/source-mysql:v3", "secret_paths": ["credentials.password", "x.y", "a"],
                                      "config": {"credentials": {"password": "abc"}, "a": ""}})
    assert checked["secrets"] == ["abc"]


# drafts

DEST_SECRET = "dest-pw-qz81-made-up"


def catalog(flag="ok"):
    return {"captures": {"acme/pg": {"endpoint": {"connector": {"image": "x", "config": {"password_sops": "ENC"}}}, "bindings": []}},
            "collections": {"acme/pg/t": {"key": ["/id"]}},
            "materializations": {"acme/md": {"source": {"capture": "acme/pg"}, "flag": flag,
                                             "endpoint": {"connector": {"image": "y", "config": {"token": DEST_SECRET}}}, "bindings": []}}}


SECRET_PATH = [["materializations", "acme/md", "endpoint", "connector", "config", "token"]]


def drafts(env):
    path = env["tool"].parent / "drafts.json"
    return json.loads(path.read_text())["drafts"] if path.exists() else {}


def author(client, flag="ok", replace=None, verb="draft_author"):
    return client.result(id="a", type=verb, profile="me", input={"catalog": catalog(flag), "secret_paths": SECRET_PATH, "replace": replace})


def test_author_makes_a_draft_with_the_secret_encrypted_and_says_what_is_in_it(env, client):
    r = author(client)
    assert r["ok"] and r["draft_id"] in drafts(env)
    assert sorted(s["catalog_name"] for s in r["specs"]) == ["acme/md", "acme/pg", "acme/pg/t"]
    assert DEST_SECRET not in json.dumps(drafts(env)) and DEST_SECRET not in json.dumps(r)
    assert_nothing_left(env, client)
    assert not any(DEST_SECRET in json.dumps(c) for c in calls(env))


def test_author_replaces_the_old_draft_rather_than_adding_to_it(env, client):
    first = author(client)["draft_id"]
    second = author(client, replace=first)["draft_id"]
    assert first != second and list(drafts(env)) == [second]


def test_a_secret_left_unencrypted_deletes_the_draft(env, tmp_path, capfd):
    client = Client(env["table"], env["snapshots"])
    r = author(client, "keep-plain")
    client.close()
    assert not r["ok"] and r["code"] == "secret_left" and drafts(env) == {}
    out, err = capfd.readouterr()
    sent = json.dumps(r) + everything_sent(client) + out + err
    for piece in fragments(DEST_SECRET):
        assert piece not in sent


@pytest.mark.parametrize("flag,code", [("author-fails", "tool_error")])
def test_a_failed_author_deletes_its_draft_and_hides_the_secret(env, client, flag, code):
    r = author(client, flag)
    assert not r["ok"] and r["code"] == code and drafts(env) == {}
    for piece in fragments(DEST_SECRET):
        assert piece not in r["error"]
    assert_nothing_left(env, client)


def test_an_author_that_hangs_times_out_and_deletes_its_draft(env, client):
    r = author(client, "author-hangs", verb="draft_author_quick")
    assert r["code"] == "timeout" and drafts(env) == {}
    assert_nothing_left(env, client)


def test_test_and_delete_select_es1s_draft_first(env, client):
    ours = author(client)["draft_id"]
    other = author(client)["draft_id"]  # As if someone selected another draft meanwhile.
    assert client.result(id="t", type="draft_test", input={"draft_id": ours})["ok"]
    assert {"tested": ours} in calls(env)
    assert client.result(id="d", type="draft_delete", input={"draft_id": ours})["ok"]
    assert list(drafts(env)) == [other]


def test_a_failed_test_says_why(env, client):
    did = author(client, "test-fails")["draft_id"]
    r = client.result(id="t", type="draft_test", input={"draft_id": did})
    assert not r["ok"] and r["error"] == "cannot connect to destination" and did in drafts(env)


def test_a_draft_thats_gone_says_so(client):
    r = client.result(id="t", type="draft_test", input={"draft_id": "00000000000000ff"})
    assert not r["ok"] and r["code"] == "no_draft"


@pytest.mark.parametrize("bad", [
    None, {}, {"catalog": {}}, {"catalog": {"tests": {}}}, {"catalog": {"captures": []}},
    {"catalog": {"captures": {"a/b": {"delete": True}}}},
    {"catalog": {"collections": {"a/b": {"reset": True}}}},
    {"catalog": catalog(), "secret_paths": "token"},
    {"catalog": catalog(), "secret_paths": SECRET_PATH, "replace": "--id"},
])
def test_a_bad_author_never_runs_and_doesnt_repeat_its_input(env, client, bad):
    client.send(id="a", type="draft_author", input=bad)
    m = client.until("a", "error")
    assert m["code"] == "bad_request" and calls(env) == [] and DEST_SECRET not in json.dumps(m)


@pytest.mark.parametrize("verb", ["draft_test", "draft_delete"])
@pytest.mark.parametrize("bad", [None, {}, {"draft_id": "x; rm"}, {"draft_id": "-a"}, {"draft_id": 7}])
def test_a_bad_draft_id_never_runs(env, client, verb, bad):
    client.send(id="x", type=verb, input=bad)
    assert client.until("x", "error")["code"] == "bad_request" and calls(env) == []


# A document's rows, and an argument's format


def test_a_document_verb_reads_one_value_and_saves_by_its_rule(client):
    r = client.result(id="r", type="document")
    assert r["rows"] == [{"n": 1, "big": {"x": 1}}, {"n": 2}] and r["bad_lines"] == 0
    assert [json.loads(line) for line in open(r["snapshot"])] == [{"n": 1}, {"n": 2}]


def test_an_argument_can_be_formatted_after_its_checked(env, client):
    client.result(id="r", type="formatted", args={"name": "abc"})
    assert calls(env)[-1] == ["rows", "--q", "x=eq.abc"]
    client.send(id="b", type="formatted", args={"name": "a;b"})
    client.until("b", "error")


# login


def fragments(token, size=6):
    return {token[i:i + size] for i in range(len(token) - size + 1)}


def everything_sent(client):
    sent = []
    while not client.messages.empty():
        sent.append(json.dumps(client.messages.get_nowait()))
    return "\n".join(sent)


def start_login(client, rid="l", verb="log_in", profile="me"):
    client.send(id=rid, type=verb, profile=profile)
    client.until(rid, "accepted")
    assert client.until(rid, "event")["event"] == "waiting"


def test_a_login_gets_the_token_at_a_terminal_and_only_reports_how_it_ended(env, client):
    start_login(client)
    client.send(id="s", type="secret", request="l", secret=FAKE_TOKEN)
    client.until("s", "ok")
    assert client.until("l", "result")["result"] == {"ok": True, "exit": 0}
    assert (env["tool"].parent / "tool-saved-it").exists()
    assert {"tty": True} in calls(env)
    assert ["-P", "me", "login"] in calls(env)  # the token isn't in its arguments


@pytest.mark.parametrize("token", [FAKE_TOKEN, "wrong." + FAKE_TOKEN[::-1]])
def test_no_piece_of_a_token_reaches_anything_saved_or_sent(env, tmp_path, capfd, token):
    client = Client(env["table"], env["snapshots"])
    start_login(client)
    client.send(id="s", type="secret", request="l", secret=token)
    client.until("s", "ok")
    result = client.until("l", "result")["result"]
    assert result["ok"] is (token == FAKE_TOKEN)
    client.close()

    sent = json.dumps(result) + everything_sent(client)
    out, err = capfd.readouterr()
    saved = "".join(p.read_text(errors="replace") for p in tmp_path.rglob("*")
                    if p.is_file() and p.name not in ("fake-tool", "table.json"))
    assert not env["snapshots"].exists()
    leaks = [(piece, where) for piece in fragments(token)
             for where, text in (("sent", sent), ("stdout", out), ("stderr", err), ("saved", saved)) if piece in text]
    assert leaks == []


def test_a_refused_token_is_a_plain_failure_and_saves_nothing(env, client):
    start_login(client)
    client.send(id="s", type="secret", request="l", secret="not-the-token")
    client.until("s", "ok")
    r = client.until("l", "result")["result"]
    assert not r["ok"] and r["code"] == "login_failed" and r["exit"] == 1
    assert "JWT" not in r["error"] and "not-the-token" not in r["error"]
    assert not (env["tool"].parent / "tool-saved-it").exists()


@pytest.mark.parametrize("secret", ["two words", "line\nbreak", "tab\there", "", 7, None, ["x"], "x" * 20_000, "ünicode"])
def test_a_bad_secret_is_refused_without_repeating_it_and_the_login_keeps_waiting(env, client, secret):
    start_login(client)
    client.send(id="s", type="secret", request="l", secret=secret)
    m = client.until("s", "error")
    assert m["code"] == "bad_secret"
    if isinstance(secret, str) and secret:
        assert secret not in json.dumps(m)
    client.send(id="g", type="secret", request="l", secret=FAKE_TOKEN)
    client.until("g", "ok")
    assert client.until("l", "result")["result"]["ok"]


def test_a_secret_needs_a_login_that_is_waiting(client):
    client.send(id="s", type="secret", request="nobody", secret=FAKE_TOKEN)
    assert client.until("s", "error")["code"] == "not_waiting"
    client.send(id="r", type="slow")
    client.until("r", "accepted")
    client.send(id="t", type="secret", request="r", secret=FAKE_TOKEN)
    m = client.until("t", "error")
    assert m["code"] == "not_waiting" and FAKE_TOKEN not in json.dumps(m)
    client.send(id="x", type="stop", request="r")
    client.until("r", "result")


def test_stop_cancels_a_login_and_nothing_is_saved(env, client):
    start_login(client)
    client.send(id="x", type="stop", request="l")
    r = client.until("l", "result")["result"]
    assert r["stopped"] and not r["ok"]
    assert not (env["tool"].parent / "tool-saved-it").exists()


def test_a_login_that_breaks_before_the_token_says_so(client):
    r = client.result(id="l", type="log_in_breaks")
    assert not r["ok"] and r["code"] == "login_failed" and "before it had a token" in r["error"]


def test_a_login_nobody_finishes_times_out(client):
    r = client.result(id="l", type="log_in_quick")
    assert not r["ok"] and r["code"] == "timeout"


def test_stopping_the_engine_ends_a_waiting_login(env):
    c = Client(env["table"], env["snapshots"])
    start_login(c)
    procs = running_children(c.server)
    c.requests.close()
    c.thread.join(10)
    assert not c.thread.is_alive() and procs[0].poll() is not None


def test_a_login_cant_be_saved(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text(json.dumps({**TABLE, "verbs": {"v": {"argv": ["v"], "mode": "login", "snapshot": {"keep": ["a"]}}}}))
    with pytest.raises(ValueError, match="never saved"):
        load_table(path)


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


def test_flowctl_table_never_publishes_for_now():
    """This PR's scope. Publishing arrives in its own PR and changes this test on purpose."""
    t = flowctl.table()
    assert set(t.verbs) == {"catalog_list", "catalog_status", "catalog_history", "raw_stats", "raw_list_shards",
                            "logs_read", "logs_follow", "auth_login", "connector_schema", "discover",
                            "draft_author", "draft_test", "draft_delete"}
    words = {word for verb in t.verbs.values() for word in verb.argv}
    words |= {a.flag for verb in t.verbs.values() for a in verb.args}
    for banned in ("publish", "token", "--token", "preview", "update", "rpc", "--models",
                   "--auto-approve", "--dangerous-auto-approve"):
        assert banned not in words
    assert [name for name, verb in t.verbs.items() if "auth" in verb.argv] == ["auth_login"]
    login = t.verbs["auth_login"]
    assert login.argv == ("auth", "login") and login.mode == "login" and login.snapshot is None and not login.args


def test_flowctl_discover_is_wired_up_and_saves_nothing():
    t = flowctl.table()
    assert t.verbs["discover"].mode == "handler" and t.verbs["discover"].snapshot is None
    assert set(flowctl.HANDLERS) == {"discover", "draft_author", "draft_test", "draft_delete"}
    assert all(t.verbs[name].mode == "handler" and t.verbs[name].snapshot is None for name in flowctl.HANDLERS)
    assert "publish" not in json.dumps([v.argv for v in t.verbs.values()])
    source = (Path(flowctl.__file__).parent / "drafts.py").read_text()
    assert '"publish"' not in source
    assert build_argv(t, t.verbs["discover"], {"data_plane": "ops/dp/public/aws-us-east-1-c1"}, "me") == [
        "--profile", "me", "discover", "--source", "flow.yaml", "--flat", "--data-plane", "ops/dp/public/aws-us-east-1-c1"]
    with pytest.raises(BadRequest):
        build_argv(t, t.verbs["discover"], {"data_plane": "elsewhere/x"})


@pytest.mark.parametrize("name,ok", [("source-postgres", True), ("materialize-iceberg", True), ("materialize-s3-parquet", True),
                                     ("materialize-postgres", False), ("source-http-ingest", False),
                                     ("source-postgres&x=1", False), ("materialize-s3-parquet,y", False)])
def test_flowctl_connector_schema_only_asks_about_panel_connectors(name, ok):
    t = flowctl.table()
    verb = t.verbs["connector_schema"]
    if ok:
        assert f"connectors.image_name=eq.ghcr.io/estuary/{name}" in build_argv(t, verb, {"image": name})
    else:
        with pytest.raises(BadRequest):
            build_argv(t, verb, {"image": name})


def test_flowctl_only_saves_stats_raw():
    assert [name for name, verb in flowctl.table().verbs.items() if verb.snapshot and verb.snapshot.raw] == ["raw_stats"]


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
