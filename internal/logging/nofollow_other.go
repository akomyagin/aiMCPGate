//go:build !unix

package logging

// oNoFollow is a no-op on platforms without O_NOFOLLOW open semantics
// (Windows and friends): the symlink defence in openAppendFile is
// best-effort, Unix-only. See nofollow_unix.go.
const oNoFollow = 0
