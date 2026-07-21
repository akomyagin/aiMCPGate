# aiMCPGate OCI image, built by goreleaser (see .goreleaser.yaml `dockers:`).
# goreleaser injects the pre-built, statically linked mcp-gate binary into the
# build context, so there is no Go build stage here.
#
# Image policy: this image contains ONLY the mcp-gate binary. It ships no
# runtimes for stdio upstreams (no node/npx, no python, no shells) — if your
# config launches stdio upstream servers, extend this image yourself and
# install what they need. HTTP upstreams work out of the box: distroless/static
# includes CA certificates, so TLS to remote MCP endpoints just works.
FROM gcr.io/distroless/static-debian12:nonroot

COPY mcp-gate /mcp-gate
# Demo-only config for registry sandbox introspection (Glama.ai etc.):
# `serve -c /demo.config.yaml` starts the gateway with the built-in
# __demo-echo stub as its only upstream. Never use it for real work.
COPY demo.config.yaml /demo.config.yaml

ENTRYPOINT ["/mcp-gate"]
CMD ["serve"]
