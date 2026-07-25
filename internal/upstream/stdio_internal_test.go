package upstream

import (
	"bytes"
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
		name:       "overlong",
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
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
