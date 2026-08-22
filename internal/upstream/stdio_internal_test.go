package upstream

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"
)

// TestDrainStderrSurvivesOverlongLine is a regression test for finding #15:
// drainStderr used to stop reading entirely once bufio.Scanner hit a permanent
// error (a single stderr "line" longer than its 1MiB token limit). The pipe
// stayed open until cmd.Wait with nobody reading it, so a child that kept
// writing would block as soon as the 64KiB OS pipe buffer filled. The fix
// switches to a raw io.Copy(io.Discard, ...) drain after a scanner error; the
// writer below must therefore complete (all bytes consumed) and stderrDone
// must close — with the old code the writer stayed blocked forever.
func TestDrainStderrSurvivesOverlongLine(t *testing.T) {
	pr, pw := io.Pipe()
	c := &stdioTransport{
		name: "overlong",
		// Debug level ON: the per-line slog path must be active alongside the
		// scanner whose failure mode this test pins down. (The scanner itself
		// now runs at EVERY level — it feeds the stderr ring — so the level no
		// longer changes which read path executes.)
		log:        slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})),
		stderr:     pr,
		stderrDone: make(chan struct{}),
	}
	go c.drainStderr()

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		// One "line" slightly longer than the scanner's 1MiB buffer limit
		// (see sc.Buffer in drainStderr) — makes Scan fail with ErrTooLong...
		_, _ = pw.Write(bytes.Repeat([]byte("a"), 1<<20+64))
		// ...then more output that only a raw drain will ever consume.
		_, _ = pw.Write([]byte("\nmore output after the scanner gave up\n"))
		_ = pw.Close()
	}()

	deadline := time.After(10 * time.Second)
	select {
	case <-writerDone:
	case <-deadline:
		t.Fatal("writer still blocked: drainStderr stopped reading after the scanner error instead of raw-draining to EOF")
	}
	select {
	case <-c.stderrDone:
	case <-deadline:
		t.Fatal("stderrDone was not closed after the pipe reached EOF")
	}
}

// TestStderrRingKeepsLastLines pins the ring-buffer semantics behind
// StderrTail: it holds at most stderrRingSize lines, evicting the OLDEST when
// full, and the snapshot comes back oldest-first. ok is true even for an
// empty ring — stdio always has a stderr, whether or not the child used it.
func TestStderrRingKeepsLastLines(t *testing.T) {
	c := &stdioTransport{}

	tail, ok := c.StderrTail()
	if !ok {
		t.Fatal("StderrTail ok = false for a stdio transport; want true even when empty")
	}
	if len(tail) != 0 {
		t.Fatalf("empty ring returned %d lines: %q", len(tail), tail)
	}

	// Fewer lines than the capacity: all of them, in write order.
	for i := 0; i < 3; i++ {
		c.appendStderrLine(fmt.Sprintf("early %d", i))
	}
	tail, _ = c.StderrTail()
	if len(tail) != 3 || tail[0] != "early 0" || tail[2] != "early 2" {
		t.Fatalf("partial ring = %q; want [early 0 early 1 early 2]", tail)
	}

	// Overflow the ring: only the last stderrRingSize lines survive.
	c = &stdioTransport{}
	total := stderrRingSize + 10
	for i := 0; i < total; i++ {
		c.appendStderrLine(fmt.Sprintf("line %d", i))
	}
	tail, ok = c.StderrTail()
	if !ok {
		t.Fatal("StderrTail ok = false after appends")
	}
	if len(tail) != stderrRingSize {
		t.Fatalf("full ring returned %d lines, want %d", len(tail), stderrRingSize)
	}
	if want := fmt.Sprintf("line %d", total-stderrRingSize); tail[0] != want {
		t.Errorf("oldest surviving line = %q, want %q", tail[0], want)
	}
	if want := fmt.Sprintf("line %d", total-1); tail[len(tail)-1] != want {
		t.Errorf("newest line = %q, want %q", tail[len(tail)-1], want)
	}

	// The snapshot is a COPY: appending after the snapshot must not mutate it.
	before := tail[0]
	for i := 0; i < stderrRingSize; i++ {
		c.appendStderrLine("overwrite")
	}
	if tail[0] != before {
		t.Error("StderrTail snapshot aliased the live ring; want an independent copy")
	}
}

// TestHTTPStderrTailReportsAbsence: the HTTP transport has no child process
// and hence no stderr — StderrTail must honestly return nil,false, exactly
// like Done does.
func TestHTTPStderrTailReportsAbsence(t *testing.T) {
	tail, ok := (&httpTransport{}).StderrTail()
	if ok || tail != nil {
		t.Fatalf("httpTransport.StderrTail() = %v, %v; want nil, false", tail, ok)
	}
}

// U4. TestIsolatedEnvAllowlistNames pins the isolated-mode allowlist by name —
// the Windows entries cannot be checked by a live run in this Linux-only
// session, so this list is their only guard. Keep it synchronized with the
// isolatedEnvAllowlist doc-comment (plan §3.3): change both together.
func TestIsolatedEnvAllowlistNames(t *testing.T) {
	want := []string{
		"PATH", "HOME", "TMPDIR",
		"SystemRoot", "windir", "ComSpec", "PATHEXT", "TEMP", "TMP", "USERPROFILE",
	}
	if len(isolatedEnvAllowlist) != len(want) {
		t.Fatalf("isolatedEnvAllowlist = %v, want %v", isolatedEnvAllowlist, want)
	}
	for i, w := range want {
		if isolatedEnvAllowlist[i] != w {
			t.Errorf("isolatedEnvAllowlist[%d] = %q, want %q", i, isolatedEnvAllowlist[i], w)
		}
	}
}

// U5. TestBaseEnvironOnlySetAllowlisted: baseEnviron includes an allowlisted
// variable that IS set (PATH), omits allowlisted names that are NOT set, and
// never emits a variable outside the allowlist.
func TestBaseEnvironOnlySetAllowlisted(t *testing.T) {
	const marker = "TEST-AIMCPGATE-FAKE-PATH-BASE-0001"
	t.Setenv("PATH", marker)
	// A non-allowlisted variable that is set must not appear in the base.
	t.Setenv("TEST_AIMCPGATE_NOT_ALLOWLISTED", "TEST-AIMCPGATE-FAKE-OUTSIDE-0002")

	base := baseEnviron()

	allowed := make(map[string]struct{}, len(isolatedEnvAllowlist))
	for _, k := range isolatedEnvAllowlist {
		allowed[k] = struct{}{}
	}
	foundPath := false
	for _, kv := range base {
		eq := -1
		for i := 0; i < len(kv); i++ {
			if kv[i] == '=' {
				eq = i
				break
			}
		}
		if eq < 0 {
			t.Fatalf("baseEnviron entry %q has no '='", kv)
		}
		key := kv[:eq]
		if _, ok := allowed[key]; !ok {
			t.Errorf("baseEnviron leaked a non-allowlisted variable %q", key)
		}
		if kv == "PATH="+marker {
			foundPath = true
		}
	}
	if !foundPath {
		t.Errorf("baseEnviron must carry the set PATH; got %v", base)
	}
}
