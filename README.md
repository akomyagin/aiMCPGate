# aiMCPGate

*Русская версия — [README_RU.md](README_RU.md).*

A gateway / proxy for **MCP servers** (Model Context Protocol) written in Go.
It presents itself to an MCP client (Claude Code, Cursor, etc.) as **one**
MCP server, while under the hood it **multiplexes** calls across several
upstream MCP servers, **aggregates** their tool/resource catalogs into one,
and **logs** every call.

> Status: **MVP complete (Stages 0–6)**. Phase 1 — multiplexing stdio
> upstreams behind a stdio endpoint with a call log; Phase 2 — HTTP/SSE
> client-facing transport, HTTP upstreams, a CLI log viewer (`mcp-gate
> logs`); release pipeline (`goreleaser`, cross-compiled for
> linux/darwin/windows × amd64/arm64, no CGO).

## Releases

Cross-platform binaries are built via [`goreleaser`](https://goreleaser.com)
(`.goreleaser.yaml`): `linux`/`darwin`/`windows` × `amd64`/`arm64`, no CGO,
the version is baked in via `-ldflags -X main.version=...`, checksums land in
`SHA256SUMS`. Local dry run: `goreleaser release --snapshot --clean`.

## Why

An active MCP user typically has several servers configured (filesystem,
GitHub, search, custom ones), each one duplicated in every client's own
config. `aiMCPGate` gives you:

- **One entry point** — a single MCP endpoint instead of N entries in the
  client config.
- **One catalog** — every upstream server's tools merged together (namespaced
  as `<upstream>__<tool>` so names never collide).
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
  (CLI/web), optionally an access policy.

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

# http mode (transport: http in the config) — endpoint at http://<listen_addr>/mcp:
mcp-gate serve --config ./config-http.yaml

# view the call log (last 50 records; filter by upstream/tool/status):
mcp-gate logs --file ./logs/calls.jsonl --tail 50
mcp-gate logs --config ./config.yaml --upstream github --status err

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
```

All commands except `token --generate` and `skill` (which falls back to a built-in
guide) load the config: pass `--config`, or drop a `config.yaml` next to the
binary (see Configuration below).

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
where only the tool `allow`/`deny`/`rename` filter changed are re-projected
without any restart. Unchanged upstreams keep running untouched. A bad edit
(invalid YAML, failed validation) is logged and ignored — the currently running
config stays live, so a typo never takes the gateway down.

**Behavioural note:** since the gateway installs a SIGHUP handler, SIGHUP no
longer terminates the process the way the OS default would. To stop the gateway
use Ctrl-C, SIGINT, or SIGTERM. SIGHUP is Unix-only; on Windows it does not
exist and reload is unavailable (the process serves the config it started with
until restarted).

## Configuration

Without `--config`, the gateway looks for `config.yaml` **next to its own
binary** (e.g. if `mcp-gate` is installed at `/etc/gate/`, it looks for
`/etc/gate/config.yaml` — regardless of the working directory it was launched
from). If that file doesn't exist and `--config` wasn't passed either, it
errors explicitly instead of starting an empty gateway. Relative paths inside
the config (`log_file`, `skill_file`) resolve against the **config file's own
directory**, not the current working directory.

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
  - name: remote            # http upstream (Phase 2)
    url: https://mcp.example.com/mcp
    headers:
      Authorization: "Bearer ${REMOTE_MCP_TOKEN}"   # secret, never logged
    enabled: true
```

## License

MIT — see [`LICENSE`](LICENSE).
