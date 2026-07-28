# aiMCPGate

*Русская версия — [README_RU.md](README_RU.md).*

A gateway / proxy for **MCP servers** (Model Context Protocol) written in Go.
It presents itself to an MCP client (Claude Code, Cursor, etc.) as **one**
MCP server, while under the hood it **multiplexes** calls across several
upstream MCP servers, **aggregates** their tools, prompts and resources into
one catalog, and **logs** every call.

> Status: **MVP complete (Stages 0–6) + post-MVP Stages 7–12 and 14 further
> rounds shipped in v0.3.0.** Phase 1 — multiplexing stdio upstreams behind a
> stdio endpoint with a call log; Phase 2 — HTTP/SSE client-facing transport,
> HTTP upstreams, a CLI log viewer (`mcp-gate logs`); release pipeline
> (`goreleaser`, cross-compiled for linux/darwin/windows × amd64/arm64, no
> CGO). Post-MVP added upstream auto-restart, hot config reload, tool
> filtering/renaming, `doctor`, and — in v0.3.0 — full `prompts`/`resources`/
> `resources/templates`/`completion` aggregation, `ping`, progress forwarding
> and real cancellation, `logging/setLevel` fan-out, per-upstream call limits
> (rate limit / concurrency / result truncation / timeout), a lazy catalog and
> `tools/list` pagination, SSE server→client streams on both the client and the
> upstream side, and `elicitation/create` proxying (stdio↔stdio only). Merged
> since v0.3.0 but not yet released: `sampling/createMessage` and `roots/list`
> proxying (stdio↔stdio as well), honest capability declaration to
> upstreams — the gateway now offers an upstream exactly what its own client
> declared, instead of a blanket `{}` — server-side `Mcp-Session-Id` sessions on
> the HTTP transport, with `DELETE /mcp` termination, and server→client requests
> over HTTP on BOTH sides — for an HTTP-connected client and from an
> HTTP-connected upstream (`elicitation`, `sampling`, `roots` — see below).
>
> **Not implemented:** a per-client access policy.

## Releases

Cross-platform binaries are built via [`goreleaser`](https://goreleaser.com)
(`.goreleaser.yaml`): `linux`/`darwin`/`windows` × `amd64`/`arm64`, no CGO,
the version is baked in via `-ldflags -X main.version=...`, checksums land in
`SHA256SUMS`. Local dry run: `goreleaser release --snapshot --clean`.

## Install from MCP registry

Besides the raw release binaries, the gateway ships as an OCI image on GitHub
Container Registry and as an npm wrapper package — the two formats MCP
registries install from.

Docker:

```bash
docker run --rm -i -v $(pwd)/config.yaml:/config.yaml ghcr.io/akomyagin/aimcpgate serve
```

`-i` is mandatory: the gateway talks MCP over stdio, so the client must keep
stdin open (without it the container sees EOF and exits immediately). The
image has no config of its own, so mount yours — the example above mounts it
onto the default path `/config.yaml`; any other path works with `serve -c`.

To reproduce a registry sandbox check (Glama.ai etc.) without any real
upstream, use the demo config baked into the image — this exact command is
what a sandbox should run:

```bash
docker run --rm -i ghcr.io/akomyagin/aimcpgate serve -c /demo.config.yaml
```

npx (downloads the prebuilt binary for your platform on first install and
verifies its SHA256 checksum):

```bash
npx aimcpgate serve -c ./config.yaml
```

**Image policy:** the OCI image contains only the `mcp-gate` binary — no
runtimes for stdio upstreams (no node/npx, python, shells). If your config
launches stdio upstream servers, extend the image yourself and install what
they need; HTTP upstreams work out of the box (CA certificates are included).

**Demo config:** [`demo.config.yaml`](demo.config.yaml) and the hidden
`__demo-echo` subcommand exist only so registry sandboxes (Glama.ai) can
introspect the gateway without any real upstream — never use them in a real
deployment.

## Why

An active MCP user typically has several servers configured (filesystem,
GitHub, search, custom ones), each one duplicated in every client's own
config. `aiMCPGate` gives you:

- **One entry point** — a single MCP endpoint instead of N entries in the
  client config.
- **One catalog** — every upstream server's tools and prompts merged together
  (namespaced as `<upstream>__<tool>` so names never collide), plus their
  resources and resource templates (addressed by URI, so never renamed).
- **A call log** — which upstream, which tool, when, success/failure. This is
  the value added on top of "just a proxy".

Solo pet project: the priority is learning Go (concurrency, `os/exec`,
JSON-RPC 2.0, the stdio and HTTP/SSE transports). Cost — **$0/month** by
default (a local process), no telemetry.

## How it works (short version)

```
MCP client ──stdio/HTTP──▶ aiMCPGate ──JSON-RPC──▶ upstream A (stdio)
                              │        ├─────────▶ upstream B (stdio)
                          call log     └─────────▶ upstream C (http, Phase 2)
```

## MVP (two phases)

- **Phase 1** — multiplexing 2+ **stdio** upstreams behind one **stdio**
  endpoint (the same transport Claude Code speaks) plus basic logging.
- **Phase 2** — **HTTP/SSE** transport, HTTP upstream servers, a log viewer
  (the CLI one was built; the web view was deliberately dropped), optionally an
  access policy — **that one was considered and declined**.

## Build

```bash
export PATH="$HOME/sdk/go/bin:$PATH"   # if go isn't already on PATH
go build ./...
go vet ./...
go test -race ./...

go run ./cmd version
```

## Usage

```bash
# stdio mode (the client launches the gateway as a subprocess):
mcp-gate serve --config ./config.yaml

# http mode (transport: http in the config) — endpoint at http://<listen_addr>/mcp;
# every request after initialize carries the issued Mcp-Session-Id (see below):
mcp-gate serve --config ./config-http.yaml

# check every enabled upstream once (launch → handshake → tools/list) and print
# a per-upstream OK/FAIL table; exit code is non-zero if any upstream failed
# (scriptable for CI/cron), no auto-restart, no call logging — one pass then exit:
mcp-gate doctor --config ./config.yaml

# call one aggregated tool once from the shell (single bring-up, no supervisor —
# the fastest way to debug a config, a filter or a rename without a live client):
mcp-gate call github__search_repositories '{"query":"mcp"}' --config ./config.yaml

# report the aggregated catalog size per upstream (tools / bytes / ~tokens) plus
# the heaviest individual tools — the data behind allow-list / strip decisions:
mcp-gate catalog --config ./config.yaml --top 20

# view the call log (last 50 records; filter by upstream/tool/status):
mcp-gate logs --file ./logs/calls.jsonl --tail 50
mcp-gate logs --config ./config.yaml --upstream github --status err
# keep watching the log as it grows, or aggregate it instead of listing records
# (--follow and --stats are mutually exclusive):
mcp-gate logs --config ./config.yaml --follow
mcp-gate logs --config ./config.yaml --stats

# generate a random auth token (for the HTTP transport) and see how to wire it in:
mcp-gate token --generate
# print the auth token currently set in the config:
mcp-gate token --config ./config-http.yaml

# print ready-to-paste MCP client config snippets (Claude Code / Cursor); requires
# transport: http in the config, and includes the Bearer header when auth_token is set:
mcp-gate client-config --config ./config-http.yaml

# print a SKILL.md teaching an agent how to use the aggregated catalog
# (built-in text by default; overridable via skill_file in the config):
mcp-gate skill > .claude/skills/mcp-gate/SKILL.md

# shell completions (cobra's built-in command; the release archives also ship
# pre-generated ones):
mcp-gate completion bash > /etc/bash_completion.d/mcp-gate
```

All commands except `token --generate`, `completion` and `skill` (which falls
back to a built-in guide) load the config: pass `--config`, or drop a
`config.yaml` next to the binary (see Configuration below).

`serve`, `doctor`, `call` and `catalog` also accept `--env-file ./.env` — a
minimal `KEY=VALUE` parser applied **before** the config is loaded, so `${VAR}`
references inside the config resolve from that file. The real process
environment always wins over the file.

### HTTP sessions (`Mcp-Session-Id`)

In http mode the gateway runs Streamable HTTP sessions: the reply to
`initialize` carries an **`Mcp-Session-Id`** header, and every request after it
— POST, the GET SSE stream, DELETE — must send that header back. Without it the
answer is **400**; with an unknown or expired id, **404**, which tells the
client to `initialize` again. A session is released by **`DELETE /mcp`** (204),
or after 30 minutes with no requests — an open SSE stream counts as activity and
keeps it alive.

MCP clients do all of this for you. For hand-made `curl` calls, take the header
from the initialize response and echo it back:

```bash
SID=$(curl -sD - -o /dev/null -X POST http://127.0.0.1:28080/mcp \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"1"}}}' \
  | tr -d '\r' | awk -F': ' '/^[Mm]cp-[Ss]ession-[Ii]d/{print $2}')

curl -s -X POST http://127.0.0.1:28080/mcp \
  -H 'Content-Type: application/json' -H "Mcp-Session-Id: $SID" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'

curl -s -X DELETE http://127.0.0.1:28080/mcp -H "Mcp-Session-Id: $SID"
```

The session also makes the call log honest: every call is audited under the
`clientInfo` of the session that made it, so several HTTP clients are told apart
in `calls.jsonl` instead of sharing one blank `client` field.

### Server→client requests over HTTP (`elicitation`, `sampling`, `roots`)

When an upstream asks something mid-call — `elicitation/create`,
`sampling/createMessage`, `roots/list` — the question is delivered as an SSE
event on the **GET `/mcp` stream of one session**, and the client answers with
an ordinary POST carrying a JSON-RPC response with the same id and the same
`Mcp-Session-Id`. Only the session the question was put to may answer it; an
answer from any other session is ignored. If nobody has declared the capability
with a stream open, the upstream is refused right away in the shape the spec
prescribes (`{"action":"decline"}` for elicitation, `-32601` for the other two)
rather than being left to time out — and the same happens if the session is
terminated while a question is outstanding.

Three consequences worth knowing:

- **The upstreams are told about the capabilities of the FIRST client that
  initializes**, and that set is fixed for the life of the process. MCP
  2025-06-18 has no re-negotiation, so a second client declaring more cannot
  change handshakes that already happened — an upstream is never promised a
  capability on behalf of a client it was not told about.
- **The upstreams start on the first request that needs them**, not when the
  gateway binds its port. That is what makes the declaration above possible at
  all: the handshake has to happen after a client has said what it supports. If
  the upstreams cannot start, the client gets a JSON-RPC `-32603` and the
  gateway exits with the error, as it did when it started them eagerly.
- **The question goes to a client that declared the capability — not
  necessarily to the one whose call provoked it.** Routing is by declared
  capability, and among the matching sessions the most recently active one
  wins; an upstream request carries nothing that says which caller it belongs
  to. With a single client (the normal case) this is invisible, but run two and
  a form raised by one client's `tools/call` can surface in the other's UI.

The **upstream** side of the same exchange works over HTTP too: a remote MCP
server reached with `url:` may ask its question as an SSE frame — either on its
long-lived `GET` stream or interleaved into the stream answering one of the
gateway's own POSTs, which is where SDK servers put an `elicitation/create`
raised inside a `tools/call`. The gateway proxies it through the same pipeline
and sends the client's answer back as one ordinary POST carrying a JSON-RPC
response **under the server's own request id**. Such an upstream is told the
gateway's client capabilities by the same honest policy as a stdio one — a
capability is offered only when the gateway's own client declared it, and
`doctor`/`call`/`catalog`, which have no client at all, keep declaring exactly
`{}`. The answer POST is not retried: an upstream that does not get it falls
back on its own timeout.

## Reloading config (SIGHUP)

The gateway reloads its configuration live on **SIGHUP** — no restart, no
dropped client connection. Edit `config.yaml` and send the signal:

```bash
kill -HUP $(pgrep -f 'mcp-gate serve')
```

On reload the gateway diffs the new config against the running upstreams and
applies the minimum change: newly added upstreams are launched, removed (or
`enabled: false`) ones are shut down, upstreams whose launch fields
(`command`/`args`/`url`/`env`/`headers`) changed are relaunched, and upstreams
where only the tool filter changed (`allow`/`deny`/`rename`, or the catalog
projection rules `strip_annotations`/`strip_output_schema`/`max_description`/
`describe`) are re-projected
without any restart. Call limits (`rate_limit`, `max_concurrent`,
`max_result_bytes`, `call_timeout` — global or per-upstream) are also applied
live: they never require a relaunch, the next call simply uses the new values.
Unchanged upstreams keep running untouched. A bad edit
(invalid YAML, failed validation) is logged and ignored — the currently running
config stays live, so a typo never takes the gateway down.

**Behavioural note:** since the gateway installs a SIGHUP handler, SIGHUP no
longer terminates the process the way the OS default would. To stop the gateway
use Ctrl-C, SIGINT, or SIGTERM.

SIGHUP is Unix-only. On Windows — or anywhere you would rather not send signals
— use the opt-in polling alternative instead:

```bash
mcp-gate serve --config ./config.yaml --watch-config        # bare flag = poll every 2s
mcp-gate serve --config ./config.yaml --watch-config=10s    # note the "=", not a space
```

It stats the config file's mtime on that interval and applies the same reload
path SIGHUP takes. Running it alongside the SIGHUP handler is safe.

## Configuration

Without `--config`, the gateway looks for `config.yaml` **next to its own
binary** (e.g. if `mcp-gate` is installed at `/etc/gate/`, it looks for
`/etc/gate/config.yaml` — regardless of the working directory it was launched
from). If that file doesn't exist and `--config` wasn't passed either, it
errors explicitly instead of starting an empty gateway. Relative paths inside
the config (`log_file`, `skill_file`, `debug_payload_log`) resolve against the
**config file's own directory**, not the current working directory.

An upstream is **enabled by default**: omit `enabled:` entirely and it is
launched like any other. To keep one out of the gateway without deleting its
config, disable it explicitly with **`enabled: false`** — it then appears
neither in `tools/list` nor in `mcp-gate doctor`'s table. Careful: a valueless
`enabled:` (or `enabled: null`) reads as *omitted*, so commenting the value out
leaves the upstream running — only the literal `false` disables it.

> Note: the "next to the binary" lookup uses the path of the running executable.
> Under `go run ./cmd ...` that executable is a throwaway build in a temp
> directory, so the default lookup will not find your `config.yaml` — pass
> `--config` explicitly when using `go run`, or run a built binary.

Full example with every field — [`config.example.yaml`](config.example.yaml).
The set of upstream servers is declared in YAML; **secrets (tokens) go through
env/`.env`** (`${VAR}` expansion at load time), never committed in the config.
Each upstream sets **exactly one** of `command` (stdio subprocess) or `url`
(HTTP server, Streamable HTTP) — the connection kind is inferred automatically.

```yaml
transport: stdio            # stdio (Phase 1) | http (Phase 2)
listen_addr: "127.0.0.1:28080"  # only used for transport: http; loopback by default
# auth_token: ${AIMCPGATE_TOKEN}  # required if you widen listen_addr past loopback
log_file: ./logs/calls.jsonl
# debug_payload_log: ./logs/payloads.jsonl  # OPT-IN, off by default: logs raw
#                                   # arguments AND results — can contain secrets
# Optional global call limits (each can be overridden per upstream):
# rate_limit: { rps: 5, burst: 2 }  # token bucket per upstream for tools/call
# max_result_bytes: 65536           # truncate oversized textual results (0 = off)
# call_timeout: 30s                 # bounds one upstream request
# How the catalog is presented to the client (both hot-reloadable):
# catalog_mode: lazy                # normal (default) | lazy: the client sees only
#                                   # gate_search_tools / gate_describe / gate_call
# page_size: 50                     # paginate tools/list (0/omitted = whole catalog;
#                                   # ignored in lazy mode)
# Auto-restart policy for crashed stdio upstreams (defaults: on, 1s→30s, 5 tries):
# restart: { enabled: true, initial_backoff: 1s, max_backoff: 30s, max_attempts: 5 }
upstreams:
  - name: filesystem        # stdio upstream
    command: npx
    args: ["-y", "@modelcontextprotocol/server-filesystem", "/home/user"]
    enabled: true
  - name: github
    command: github-mcp-server
    env:
      GITHUB_TOKEN: ${GITHUB_TOKEN}   # from the environment, not hardcoded
    enabled: true
    # Optional per-upstream tool filter / catalog projection (keys are ORIGINAL
    # tool names; all editable live via SIGHUP with no upstream restart):
    # tools:
    #   allow: ["search_repositories"]  # if non-empty, only these survive
    #   deny: ["delete_repository"]     # always subtracted, even from allow
    #   rename: { search_repositories: "gh_search" }
    #   strip_annotations: true         # drop heavyweight catalog fields
    #   strip_output_schema: true
    #   max_description: 200            # truncate descriptions to N runes
    #   describe: { get_issue: "Fetch one issue." }   # replace wholesale
    # Optional per-upstream call limits (override the globals for this upstream):
    # rate_limit: { rps: 1, burst: 1 }  # rps: 0 disables the global limit here
    # max_concurrent: 4                 # cap on simultaneous in-flight calls
    # max_result_bytes: 32768           # 0 disables the global cap here
    # call_timeout: 120s                # this upstream is slow — give it longer
  - name: remote            # http upstream (Phase 2)
    url: https://mcp.example.com/mcp
    headers:
      Authorization: "Bearer ${REMOTE_MCP_TOKEN}"   # secret, never logged
    enabled: true
```

## License

MIT — see [`LICENSE`](LICENSE).
