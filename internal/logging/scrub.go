package logging

import (
	"sort"
	"strings"
)

// minScrubLen is the shortest secret value the Scrubber will redact. Values
// shorter than this are left alone: "true", "1", "debug", "info", ports, short
// words are all legitimate ${VAR} values in an upstream's env: block
// (LOG_LEVEL: ${LOG_LEVEL}), and redacting them would corrupt arbitrary
// operator strings on an accidental substring hit. Real tokens are 20+ bytes.
// A secret shorter than this threshold is deliberately not protected — the cost
// paid against false positives (plan §5.2, residual limitation §11).
const minScrubLen = 8

// Scrubber replaces KNOWN secret values with "***" in operator-facing free
// text (a crashed upstream's stderr tail, audit error strings, event details).
// It is the VALUE-based complement of MaskSecrets, which masks by KEY NAME in
// the opt-in JSON payload log and deliberately stays shallow — the two
// mechanisms serve different sinks and neither replaces the other. A nil
// Scrubber and a Scrubber built from zero candidates are both valid and scrub
// nothing at zero cost.
type Scrubber struct{ r *strings.Replacer }

// NewScrubber builds a Scrubber from candidate secret values: empty strings and
// values shorter than minScrubLen are dropped (see minScrubLen), the rest are
// deduped and sorted by DESCENDING length before the Replacer is built —
// strings.Replacer tries patterns in argument order at each position, so the
// sort guarantees a secret that contains another secret as a substring is
// replaced whole, not corrupted from the inside. Zero surviving candidates
// yield a no-op Scrubber (nil inner Replacer — nothing is allocated per call).
func NewScrubber(values []string) *Scrubber {
	seen := make(map[string]struct{}, len(values))
	kept := make([]string, 0, len(values))
	for _, v := range values {
		if len(v) < minScrubLen {
			continue
		}
		if _, dup := seen[v]; dup {
			continue
		}
		seen[v] = struct{}{}
		kept = append(kept, v)
	}
	if len(kept) == 0 {
		return &Scrubber{r: nil}
	}
	// Descending length: strings.Replacer matches patterns in argument order at
	// each position, so a longer secret that contains a shorter one as a
	// substring must come first to be replaced whole. Length tie broken by
	// value for a deterministic Replacer.
	sort.Slice(kept, func(i, j int) bool {
		if len(kept[i]) != len(kept[j]) {
			return len(kept[i]) > len(kept[j])
		}
		return kept[i] < kept[j]
	})
	pairs := make([]string, 0, len(kept)*2)
	for _, v := range kept {
		pairs = append(pairs, v, "***")
	}
	return &Scrubber{r: strings.NewReplacer(pairs...)}
}

// Scrub returns text with every known secret value replaced by "***".
// Nil-receiver- and no-op-safe: returns text verbatim (the same string, no
// allocation) when there is nothing to scrub.
func (s *Scrubber) Scrub(text string) string {
	if s == nil || s.r == nil || text == "" {
		return text
	}
	return s.r.Replace(text)
}
