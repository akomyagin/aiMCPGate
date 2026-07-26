// Call-limit guards for forwarded tools/call requests (Round 6): per-upstream
// token-bucket rate limiting (golang.org/x/time/rate), per-upstream
// concurrency caps (golang.org/x/sync/semaphore) and opt-in truncation of
// oversized textual results. All three read their settings from the LIVE
// config on every call (via r.config()), so a SIGHUP reload changes limits
// without relaunching any upstream — the limiter/semaphore instances are the
// only cached state, and they are rebuilt lazily when their config values
// change (see limiterFor/semFor).

package registry

import (
	"context"
	"encoding/json"
	"fmt"

	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// limiterEntry caches one upstream's token-bucket limiter together with the
// config values it was built from, so limiterFor can detect a reload that
// changed them and rebuild. Guarded by r.limiterMu.
type limiterEntry struct {
	rps   float64
	burst int
	lim   *rate.Limiter
}

// semEntry is limiterEntry's counterpart for the concurrency semaphore.
// Guarded by r.limiterMu.
type semEntry struct {
	n   int
	sem *semaphore.Weighted
}

// limiterFor returns the named upstream's rate limiter, creating or replacing
// it when the effective config changed since the last call (lazy, under
// limiterMu). nil means no rate limit applies to this upstream. Replacing a
// limiter on a config change deliberately resets the bucket: tokens accrued
// under the old parameters do not carry over, which is the simple, explainable
// behaviour for an operator who just edited the limit.
func (r *Registry) limiterFor(name string) *rate.Limiter {
	rps, burst, ok := r.config().EffectiveRateLimitFor(name)

	r.limiterMu.Lock()
	defer r.limiterMu.Unlock()
	if !ok {
		// No limit configured (any more): drop a stale entry so a removed
		// upstream / disabled limit does not keep dead state around.
		delete(r.limiters, name)
		return nil
	}
	e := r.limiters[name]
	if e == nil || e.rps != rps || e.burst != burst {
		e = &limiterEntry{rps: rps, burst: burst, lim: rate.NewLimiter(rate.Limit(rps), burst)}
		r.limiters[name] = e
	}
	return e.lim
}

// semFor is limiterFor's counterpart for the max_concurrent semaphore. nil
// means no concurrency cap applies. When a reload changes the cap, the fresh
// semaphore starts with zero permits held — calls still in flight hold permits
// of the OLD semaphore and release them there as they finish, which is
// harmless (nothing waits on the old instance any more).
func (r *Registry) semFor(name string) *semaphore.Weighted {
	n := r.config().EffectiveMaxConcurrentFor(name)

	r.limiterMu.Lock()
	defer r.limiterMu.Unlock()
	if n <= 0 {
		delete(r.sems, name)
		return nil
	}
	e := r.sems[name]
	if e == nil || e.n != n {
		e = &semEntry{n: n, sem: semaphore.NewWeighted(int64(n))}
		r.sems[name] = e
	}
	return e.sem
}

// truncationMarker renders the text appended to a truncated result so the
// model/user can see data was cut by the gateway, not by the tool.
func truncationMarker(limit int) string {
	return fmt.Sprintf("\n[truncated by mcp-gate: result exceeded %d bytes]", limit)
}

// truncateResult shrinks an oversized successful tool result toward limit
// bytes by cutting the TEXT content blocks (in place, resp.Result is
// replaced). Results that do not parse into the standard tools/call shape
// ({"content":[{"type":"text","text":...},...]}) — or that carry no text
// blocks at all (e.g. pure image content) — are passed through UNCHANGED with
// a warning: guessing at an unknown format would break the gateway's
// transparency contract, an honest oversized result beats a mangled one.
// Errors (resp.Error) are never truncated — only successful results are.
func (r *Registry) truncateResult(resp *mcp.Message, upstream string, limit int) {
	if resp == nil || resp.Error != nil || len(resp.Result) <= limit {
		return
	}
	out, ok := truncateToolResult(resp.Result, limit)
	if !ok {
		r.log.Warn("tool result exceeds max_result_bytes but is not truncatable, passing through unchanged",
			"upstream", upstream, "bytes", len(resp.Result), "limit", limit)
		return
	}
	r.log.Debug("tool result truncated",
		"upstream", upstream, "bytes", len(resp.Result), "truncated_bytes", len(out), "limit", limit)
	resp.Result = out
}

// truncateToolResult is the pure transformation behind truncateResult: given a
// raw tools/call result larger than limit, it returns a re-marshaled copy
// whose text blocks are cut so the whole result lands close to limit, with a
// truncation marker appended to the first cut block. ok=false means the
// result is not in the expected shape (or has no text to cut) and the caller
// must pass the original through unchanged.
//
// Every field the gateway does not understand is preserved: blocks and the
// top-level result are decoded into map[string]json.RawMessage, not into a
// closed struct, so isError, structuredContent, annotations, _meta and
// anything future survive verbatim (only JSON formatting/key order may change
// on re-marshal). Text is cut on rune boundaries, never mid-UTF-8-sequence.
func truncateToolResult(result json.RawMessage, limit int) (out json.RawMessage, ok bool) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(result, &top); err != nil || top == nil {
		return nil, false
	}
	var rawBlocks []json.RawMessage
	if err := json.Unmarshal(top["content"], &rawBlocks); err != nil {
		return nil, false
	}
	blocks := make([]map[string]json.RawMessage, len(rawBlocks))
	texts := map[int]string{} // block index → original text of a text block
	var textIdx []int         // indexes of text blocks, in content order
	for i, rb := range rawBlocks {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(rb, &m); err != nil || m == nil {
			return nil, false
		}
		blocks[i] = m
		var typ, text string
		if json.Unmarshal(m["type"], &typ) == nil && typ == "text" &&
			json.Unmarshal(m["text"], &text) == nil {
			texts[i] = text
			textIdx = append(textIdx, i)
		}
	}
	if len(textIdx) == 0 {
		return nil, false // nothing cuttable: images/audio only — pass through.
	}

	remarshal := func() (json.RawMessage, bool) {
		content, err := json.Marshal(blocks)
		if err != nil {
			return nil, false
		}
		top["content"] = content
		b, err := json.Marshal(top)
		if err != nil {
			return nil, false
		}
		return b, true
	}

	// Baseline: the result with every text emptied. Whatever the baseline
	// costs (schema overhead, non-text blocks) is not ours to cut; the byte
	// budget for text is what remains up to the limit.
	for _, i := range textIdx {
		blocks[i]["text"] = mustJSONString("")
	}
	base, ok := remarshal()
	if !ok {
		return nil, false
	}
	budget := limit - len(base)

	// Hand each text block its original text back, in order, as long as it
	// fits the remaining budget; the first block that does not fit is cut to
	// fit (reserving room for the marker) and every later text stays empty.
	marker := truncationMarker(limit)
	cut := false
	for _, i := range textIdx {
		if cut {
			continue // budget exhausted at an earlier block; this one stays "".
		}
		enc := len(mustJSONString(texts[i])) - 2 // bytes beyond the `""` already counted in base
		if enc <= budget {
			blocks[i]["text"] = mustJSONString(texts[i])
			budget -= enc
			continue
		}
		room := budget - (len(mustJSONString(marker)) - 2)
		blocks[i]["text"] = mustJSONString(trimToEncodedBudget(texts[i], room) + marker)
		cut = true
	}
	if !cut {
		// Every text fit after all: the original only exceeded the limit by
		// JSON formatting the re-marshal compacted away. Nothing was lost, so
		// no marker — just return the (now small enough) compact form.
		return remarshal()
	}
	return remarshal()
}

// trimToEncodedBudget returns the longest rune-prefix of s whose JSON string
// encoding (minus the two enclosing quotes) fits within budget bytes. Binary
// search over the rune count: encoded length grows monotonically with the
// prefix, and escaping makes bytes-per-rune non-uniform, so counting bytes
// directly would over- or under-shoot.
func trimToEncodedBudget(s string, budget int) string {
	if budget <= 0 {
		return ""
	}
	runes := []rune(s)
	lo, hi := 0, len(runes)
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if len(mustJSONString(string(runes[:mid])))-2 <= budget {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return string(runes[:lo])
}

// guardedCall applies the per-upstream call-limit guards around one forwarded
// tools/call, in order: (1) rate limit — Wait respects ctx, so a client
// cancellation or deadline aborts the queueing honestly; (2) concurrency cap —
// same ctx semantics for Acquire; (3) the RPC itself under the per-upstream
// call timeout; (4) opt-in truncation of an oversized successful result. It is
// the body of callUpstream, split out so the audit/payload records in
// callUpstream cover every outcome, guard rejections included.
func (r *Registry) guardedCall(ctx context.Context, conn Upstream, rt route, arguments, meta json.RawMessage) (*mcp.Message, error) {
	if lim := r.limiterFor(rt.upstream); lim != nil {
		if err := lim.Wait(ctx); err != nil {
			return nil, fmt.Errorf("rate limit: %w", err)
		}
	}
	if sem := r.semFor(rt.upstream); sem != nil {
		if err := sem.Acquire(ctx, 1); err != nil {
			return nil, fmt.Errorf("concurrency limit: %w", err)
		}
		defer sem.Release(1)
	}

	var resp *mcp.Message
	err := r.withCallTimeoutFor(ctx, rt.upstream, func(ctx context.Context) error {
		var err error
		resp, err = conn.CallTool(ctx, rt.original, arguments, meta)
		return err
	})
	if err == nil {
		if limit := r.config().EffectiveMaxResultBytesFor(rt.upstream); limit > 0 {
			r.truncateResult(resp, rt.upstream, limit)
		}
	}
	return resp, err
}
