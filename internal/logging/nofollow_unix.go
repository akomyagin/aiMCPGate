//go:build unix

package logging

import "syscall"

// oNoFollow makes openAppendFile refuse to follow a symlink at the log path
// (the open fails with ELOOP) — a planted symlink could otherwise redirect
// gateway logs into an arbitrary file. The os package exposes no O_NOFOLLOW,
// hence the syscall constant behind a build tag.
const oNoFollow = syscall.O_NOFOLLOW
