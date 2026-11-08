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
- **The tool keeps its own login.** The engine doesn't read, store or pass credentials.
  Today it has no login command at all: run `flowctl auth login` in a terminal.

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

## Snapshots

A successful `once` result saves the tool's raw stdout to
`~/Library/Application Support/Duck Harness/Snapshots/<device>/<time>_<verb>.jsonl`, with the
time in UTC. Only you can read the folder and files. Nothing reads them yet. DuckDB can:

```sql
SELECT * FROM read_json('~/Library/Application Support/Duck Harness/Snapshots/es1/*_catalog_list.jsonl',
                        format = 'newline_delimited', union_by_name = true, filename = true);
```

Verbs that could carry secrets or customer data set `"snapshot": false`. Log follows do,
because a connector's error can quote the data it was handling.

## Development

```
uv run pytest
uv run es1-serve
```

`ES1_FLOWCTL` points the engine at a specific flowctl. Otherwise it looks on the path, then
in `/usr/local/bin`.
