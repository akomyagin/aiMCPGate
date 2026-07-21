# aimcpgate (npm wrapper)

npm wrapper for [aiMCPGate](https://github.com/akomyagin/aiMCPGate) — an MCP
gateway/proxy written in Go: one MCP endpoint that multiplexes calls across
several upstream MCP servers, aggregates their tool catalogs, and logs every
call.

This package contains no code of its own: on install it downloads the
prebuilt `mcp-gate` binary for your platform from the project's GitHub
Releases and verifies its SHA256 checksum.

```bash
npx aimcpgate serve -c ./config.yaml
```

Full documentation, configuration reference, and sources:
<https://github.com/akomyagin/aiMCPGate>.

License: MIT.
