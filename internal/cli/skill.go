package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/akomyagin/aiMCPGate/internal/config"
)

// skillBody is a generic, deployment-independent guide for an AI agent talking
// to mcp-gate. It intentionally does NOT enumerate specific upstreams or tools:
// the aggregated catalog varies per deployment and is only known at runtime, so
// baking it into a static file would go stale the moment upstreams change. The
// agent is instead taught to discover the live catalog itself via tools/list —
// this file only explains the conventions and pitfalls of doing that well.
const skillBody = `---
name: mcp-gate
description: How to work with tools exposed through mcp-gate, an MCP aggregator that multiplexes several upstream MCP servers behind one endpoint. Use whenever tool names look like "<upstream>__<tool>" or the user mentions mcp-gate.
---

# SKILL: mcp-gate — using the aggregated MCP endpoint

mcp-gate sits between you and several upstream MCP servers (GitLab, Jira,
Grafana, ...). You reach all of them through one endpoint; mcp-gate merges
their tool catalogs into one.

## Namespacing

Every tool you see is namespaced as ` + "`<upstream>__<tool>`" + `, e.g.
` + "`gitlab__search_repositories`" + `, ` + "`grafana__query_prometheus`" + `. The prefix before
` + "`__`" + ` tells you which upstream owns the tool — use it to reason about which
system a call will actually touch.

## The catalog is live, not memorized

Always call ` + "`tools/list`" + ` to see what's currently available — do not assume a
tool from a previous session still exists, or that a tool the user mentions is
present. The set of upstreams is operator-configured and varies between
deployments and over time.

## The catalog is live and can change while you work

mcp-gate's aggregated catalog is NOT a frozen startup snapshot. It changes at
runtime: an upstream that crashes is auto-restarted and its tools come back; an
upstream that announces new tools has them re-listed; and the operator can
reload the config (add/remove/change upstreams) without restarting mcp-gate. An
upstream that is down at a given moment is simply absent — there is no
broken/disabled placeholder to filter out.

Over stdio, mcp-gate advertises ` + "`listChanged: true`" + ` and sends you a
` + "`notifications/tools/list_changed`" + ` whenever the catalog changes — call
` + "`tools/list`" + ` again when you receive one to refresh what you know. Over the
HTTP transport there is no server→client channel, so you will NOT get that
notification; instead just re-run ` + "`tools/list`" + ` when a tool you expect is
missing.

So: if a tool you expect is missing, re-check ` + "`tools/list`" + ` first (it may be
under a different upstream prefix than you assumed, or a just-crashed upstream
may be mid-restart — give it a moment and list again). Don't assume a missing
tool is gone forever, and don't tell the operator to restart mcp-gate just to
pick up a newly enabled server — a config reload (SIGHUP) does that live.

## Picking between upstreams with overlapping capabilities

Several upstreams can expose similarly-named tools (e.g. more than one may
have something called "search"). Pick by domain, not by name: a GitLab-prefixed
tool searches code/issues/merge requests, a Jira/Confluence-prefixed tool
searches tickets/docs, a Grafana-prefixed tool searches dashboards/metrics/logs.
When genuinely ambiguous, ask the user which system they mean rather than
guessing.

## Don't route around the gateway

Even if an upstream's raw URL leaks into a log or error message, call it
through mcp-gate, not directly. mcp-gate is the single point of auth and audit
logging for every call — bypassing it defeats both.
`

func newSkillCmd() *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "skill",
		Short: "Print a SKILL.md teaching an agent how to use mcp-gate's aggregated catalog",
		Long: "By default prints a deployment-independent skill guide: namespacing convention,\n" +
			"why the catalog only reflects upstreams reachable at startup, and how to pick\n" +
			"between upstreams with overlapping tools. Set skill_file in the config to print\n" +
			"your own file instead (e.g. to add org-specific policy notes or a translation) —\n" +
			"copy this command's default output as a starting point.\n\n" +
			"Typical use: mcp-gate skill > .claude/skills/mcp-gate/SKILL.md",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(configPath)
			if err != nil {
				if configPath == "" {
					// No --config given and no default config next to the
					// binary either: unlike serve, skill needs no real
					// deployment to be useful, so fall back to the built-in
					// text instead of erroring. An explicit --config that
					// doesn't exist still errors below.
					cmd.Print(skillBody)
					return nil
				}
				return fmt.Errorf("load config: %w", err)
			}
			if cfg.SkillFile == "" {
				cmd.Print(skillBody)
				return nil
			}
			data, err := os.ReadFile(cfg.SkillFile)
			if err != nil {
				return fmt.Errorf("read skill_file %q: %w", cfg.SkillFile, err)
			}
			cmd.Print(string(data))
			return nil
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "path to the YAML config file")
	return cmd
}
