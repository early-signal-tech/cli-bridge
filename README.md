# cli-bridge

Engines for Duck Harness devices that wrap a command-line tool. An app starts one in the
background and talks to it over JSON Lines on stdin/stdout. It has no UI of its own.

| Device | Tool | Start command | Table |
|---|---|---|---|
| ES-1 | flowctl (Estuary) | `es1-serve` | [`cli_bridge/tools/flowctl/table.json`](cli_bridge/tools/flowctl/table.json) |
| PB-1 | [parkbench](https://github.com/early-signal-tech/parkbench) (DuckLake benchmark) | `pb1-serve` | [`cli_bridge/tools/parkbench/table.json`](cli_bridge/tools/parkbench/table.json) |

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

## Discovering

`flowctl discover` only reads a spec from a file, so a connector's config, passwords included,
has to be on disk while it runs. It's the one command that holds real secrets:

```
-> {"id": "8", "type": "discover", "profile": "default", "args": {"data_plane": "ops/dp/public/…"},
    "input": {"name": "acme/pg", "image": "ghcr.io/estuary/source-postgres:v3",
              "config": {...}, "secret_paths": ["credentials.password"]}}
<- {"id": "8", "type": "accepted"}
<- {"id": "8", "type": "result", "result": {"ok": true, "catalog": {...}, "bindings": [...]}}
```

On success flowctl swaps the secrets for encrypted ones (Estuary's KMS). On failure the plain
text stays in the file. So:

- It runs in a private folder under `Duck Harness/Work`, deleted however the run ends. A crash
  can't delete anything, so a folder over an hour old is swept by the next engine or discover.
- Errors have the secrets taken out, and no refusal repeats the input.
- The result is checked. A secret still in it, or a config that isn't encrypted, returns
  `secret_left` and nothing else.
- Nothing is snapshotted, and an unexpected failure is reported with no detail.

## Drafts

`draft_author`, `draft_test`, `draft_delete` and `draft_publish` work on ES-1's own draft, by ID. flowctl keeps one
selected draft per profile, and you might be using flowctl at the same time, so each step selects
ES-1's draft first.

`draft_author` makes a new draft holding the catalog it's given, deleting the one it `replace`s,
so nothing stale stays in it. A destination's secrets are in that catalog as plain text until
Estuary encrypts them on authoring, so it's written in a private folder like discover's. The
stored draft is then read back, and if any secret named in `secret_paths` (lists of keys into the
catalog) is still in it, the draft is deleted and the result is `secret_left`. A step that fails
or is stopped part way deletes the draft it made. A catalog that deletes or resets a spec is
refused.

`draft_publish` is the one command that changes anything live. It takes the draft and the
catalog names the person confirmed (`expect`), reads the draft back, and publishes only if it
holds exactly those names and nothing in it deletes or resets a spec. flowctl then tests again
and publishes only if the tests pass. The engine doesn't ask anyone: the host confirms each
publish before the request reaches it (Duck Harness's dh-host), so no face can skip that.
`--auto-approve` and `catalog publish` are never used.

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

`connector_schema` saves only the connector's name and tag. `auth_login` and `discover` hold
secrets, so they save nothing, and a table that says otherwise won't load.

flowctl's rules leave out publishers' emails and names, connector config, and error details.
Failing documents appear in a log line's details, so logs keep the time, level, shard, the
first line of the message and numeric fields. This can't catch everything: a connector could
quote a value in that first line. `_meta.uuid` is kept so a follow and a `logs_read` over the
same time can be de-duplicated. `logs_read` catches up on what happened while nothing was
following.

## Parkbench (PB-1)

`pb1-serve` runs Parkbench's `setup`, `reset`, `run`, and `secrets list` / `create` / `drop`.

- **`run`** is a stream: it runs `parkbench run --output jsonl`, and each line is an event
  (`batch`, `tick`, `drift`, `flush`, then `done`). `stop` interrupts it like Ctrl-C, and
  Parkbench still writes `done` with reason `interrupted`. Its snapshot keeps only the events'
  numbers and names, not a drift's error text.
- **Arguments** are PB-1's controls, each typed: the catalog (`metadata_store`, or a
  `ducklake_secret` by name), the sink, the schema, the rates and sizes. A connection string,
  a data path or S3 keys can't be passed. Switches are one word (`--migrate=true`), so a value
  can't become an argument.
- **Its own folder.** Parkbench keeps a local catalog's files where it runs (`./ducklake_data`),
  and a local `reset` deletes them there. So the engine runs it in
  `~/Library/Application Support/Duck Harness/Data/pb1` (`PB1_DATA_DIR` overrides it), never
  in a checkout.
- **`reset`** passes `--force`: Parkbench's own prompt can't be answered with stdin closed,
  and the host asks the person before the request reaches the engine.
- **`secrets_create`** sends the values in `input` (`{"values": {...}}`) to
  `parkbench secrets create --type T --name N` on its stdin, so they never become an argument.
  Nothing is saved. If Parkbench's message quotes a password, secret or key, the result says
  only that it was withheld.
- **The local DuckDB catalog goes through a Quack server**, so runs and a DuckDB terminal
  (PB-1's QUERY) can use it at once. DuckDB lets one process open the `.ducklake` file for
  writing, so the engine runs `parkbench quack-serve` to own it, on `quack:localhost:9495`
  (Parkbench's default), and requests with `metadata_store: "quack"` attach through it. It
  starts the first time something needs it: one of those requests, or `quack_start`, which
  returns the init file a terminal opens with (`duckdb -init query.sql`, in the engine's
  folder). It stops when the engine does. The token is new for each engine and reaches
  Parkbench only as `PARKBENCH_QUACK_TOKEN`. The init file holds it too, so it's readable only
  by you and deleted when the server stops. A server a crashed engine left behind is stopped
  first. If it can't start (say, the port is taken), the request ends with `quack_unavailable`.
- Parkbench doesn't report a version yet, so `hello` says it's untested.

`PB1_PARKBENCH` points the engine at a specific parkbench. Otherwise it looks on the path,
then in `/usr/local/bin`.

## Development

```
uv run pytest
uv run es1-serve
uv run pb1-serve
```

`ES1_FLOWCTL` points the engine at a specific flowctl. Otherwise it looks on the path, then
in `/usr/local/bin`.

## License

MIT. See [LICENSE](LICENSE).
