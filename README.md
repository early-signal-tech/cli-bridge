# cli-bridge

Engines for Duck Harness devices that wrap a command-line tool. An app starts one in the
background and talks to it over JSON Lines on stdin/stdout. It has no UI of its own.

| Device | Tool | Start command | Table |
|---|---|---|---|
| ES-1 | flowctl (Estuary) | `es1-serve` | [`cli_bridge/tools/flowctl/table.json`](cli_bridge/tools/flowctl/table.json) |

## Layout

- `cli_bridge/`: the shared parts. The protocol, running commands, streaming, stop and
  snapshots. Nothing here knows about a specific tool.
- `cli_bridge/tools/<tool>/`: one folder per tool. Its table lists the only commands the
  engine may run, the rules for their arguments, and the versions it's been tested on.

## The rules

- **Only listed commands run.** A request can't add a flag, change a subcommand or reach
  anything the table doesn't name.
- **Every argument is checked.** Each has a type with a pattern, and no value can start
  with `-`, so a value can't turn into a flag.
- **No shell.** Commands run directly, with stdin closed.
- **Snapshots hold only what a verb's rule names.** See below.
- **The tool keeps its own login.** The engine doesn't read or store credentials. See
  [Logging in](#logging-in).

## Protocol

Every request has an `id` and a `type`, and every message names its request's `id`.

```
-> {"id": "1", "type": "hello"}
<- {"id": "1", "type": "hello", "protocol": 1, "version": "0.1.0", "device": "es1",
    "tool": {"name": "flowctl", "path": "…", "version": "0.6.13", "tested": ["0.6.13"],
             "status": "tested", "warning": null},
    "verbs": {"catalog_list": {"mode": "once", "args": {...}}, ...}}

-> {"id": "2", "type": "catalog_status", "args": {"names": ["acme/task"]}, "profile": "default"}
<- {"id": "2", "type": "accepted"}
<- {"id": "2", "type": "result", "result": {"ok": true, "rows": [...], "count": 1,
    "truncated": false, "bad_lines": 0, "snapshot": "…/20261108T165702Z_catalog_status.jsonl"}}

-> {"id": "3", "type": "logs_follow", "args": {"task": "acme/task"}}
<- {"id": "3", "type": "accepted"}
<- {"id": "3", "type": "event", "event": "line", "line": {...}}   (any number)
-> {"id": "s", "type": "stop", "request": "3"}
<- {"id": "s", "type": "ok"}
<- {"id": "3", "type": "result", "result": {"ok": true, "lines": 12, "stopped": true}}

-> {"id": "9", "type": "shutdown"}
<- {"id": "9", "type": "ok"}
```

A request that can't start (an unknown type, a bad argument, the tool isn't installed) gets
`{"type": "error", "error": "…", "code": "…"}` in place of `accepted`. A command that starts
and then fails ends with a result of `{"ok": false, "error": "…", "code": "…", "exit": 1}`.
`code` comes from the tool's table where one matches, such as `not_logged_in`. End of input
stops everything and exits.

## Logging in

`flowctl auth login` opens its token page and waits for a token at a terminal. So a `login`
verb runs it on a private pseudo-terminal, and hands over the token when the person has it:

```
-> {"id": "7", "type": "auth_login", "profile": "default"}
<- {"id": "7", "type": "accepted"}
<- {"id": "7", "type": "event", "event": "waiting"}
-> {"id": "s", "type": "secret", "request": "7", "secret": "…"}
<- {"id": "s", "type": "ok"}
<- {"id": "7", "type": "result", "result": {"ok": true, "exit": 0}}
```

flowctl saves the token in its own config (mode 600), and the engine keeps no copy. flowctl's
terminal output repeats the token in pieces, so the engine reads that output and throws it away
without looking. The result comes only from the exit code. The token never appears in an
argument, a log, a snapshot or a message back, and no error repeats it. `stop` cancels a login.

## Snapshots

Results are saved to
`~/Library/Application Support/Duck Harness/Snapshots/<device>/<time>_<verb>.jsonl`, with the
time in UTC. Only you can read the folder and files. Nothing reads them yet. DuckDB can:

```sql
SELECT * FROM read_json('~/Library/Application Support/Duck Harness/Snapshots/es1/*_catalog_list.jsonl',
                        format = 'newline_delimited', union_by_name = true, filename = true);
```

An agent may read these files, and what it reads leaves this machine. So each verb's table
entry has to say what it saves, and a table that leaves that out won't load:

| Rule | Saves |
|---|---|
| `"snapshot": false` | Nothing. |
| `"snapshot": {"keep": [...], "first_line": [...], "numbers": [...]}` | Only these dotted paths. `keep` copies a value whole, `first_line` takes a text's first line (200 characters at most), and `numbers` takes an object's numeric values. A line that isn't JSON is never saved. |
| `"snapshot": {"raw": true}` | stdout as it came, for output that can't carry secrets or customer data. |

The face still gets every row whole, so it can show detail live. A stream's rule applies line
by line, and a stream that sent nothing leaves no file.

flowctl's rules leave out publishers' emails and names, connector config, and error details.
Failing documents appear in a log line's details, so logs keep the time, level, shard, the
first line of the message and numeric fields. This can't catch everything: a connector could
quote a value in that first line. `_meta.uuid` is kept so a follow and a `logs_read` over the
same time can be de-duplicated. `logs_read` catches up on what happened while nothing was
following.

## Development

```
uv run pytest
uv run es1-serve
```

`ES1_FLOWCTL` points the engine at a specific flowctl. Otherwise it looks on the path, then
in `/usr/local/bin`.
