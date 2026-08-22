package logging

import "testing"

// All "secret" values here are synthetic markers (TEST-AIMCPGATE-FAKE-SECRET-*),
// never real or realistic secret material.

func TestScrubReplacesKnownValues(t *testing.T) {
	const a = "TEST-AIMCPGATE-FAKE-SECRET-ALPHA-0001"
	const b = "TEST-AIMCPGATE-FAKE-SECRET-BETA-0002"
	s := NewScrubber([]string{a, b})
	in := "start " + a + " middle " + b + " again " + a + " end"
	got := s.Scrub(in)
	want := "start *** middle *** again *** end"
	if got != want {
		t.Fatalf("Scrub() = %q, want %q", got, want)
	}
}

func TestScrubIgnoresShortAndEmpty(t *testing.T) {
	s := NewScrubber([]string{"true", "1", "", "short"})
	in := "level=true count=1 mode=short empty= done"
	got := s.Scrub(in)
	if got != in {
		t.Fatalf("Scrub() = %q, want verbatim %q", got, in)
	}
}

func TestScrubLongerSecretWinsOverSubstring(t *testing.T) {
	// A is a prefix of B, so at B's start position strings.Replacer sees both
	// patterns match and picks by argument order. Text contains only B; B must
	// be replaced whole with no A-replacement leaving B's suffix behind (pins
	// the descending-length sort — longer B must precede A in the Replacer).
	const a = "TEST-AIMCPGATE-FAKE-SECRET-PREFIX"
	const b = "TEST-AIMCPGATE-FAKE-SECRET-PREFIX-AND-LONGER-TAIL"
	s := NewScrubber([]string{a, b})
	in := "before " + b + " after"
	got := s.Scrub(in)
	want := "before *** after"
	if got != want {
		t.Fatalf("Scrub() = %q, want %q (substring secret corrupted the longer one)", got, want)
	}
}

func TestScrubNoopWhenNoCandidates(t *testing.T) {
	const in = "nothing here should change TEST-AIMCPGATE-FAKE-SECRET-XYZ"

	s := NewScrubber(nil)
	if got := s.Scrub(in); got != in {
		t.Fatalf("NewScrubber(nil).Scrub() = %q, want verbatim %q", got, in)
	}

	var nilScrubber *Scrubber
	if got := nilScrubber.Scrub(in); got != in {
		t.Fatalf("(*Scrubber)(nil).Scrub() = %q, want verbatim %q", got, in)
	}
}
