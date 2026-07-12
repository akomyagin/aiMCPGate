//go:build !windows

package cli

import (
	"os"
	"syscall"
)

// reloadSignals returns the OS signals that trigger a live config reload. On
// Unix that is SIGHUP, the conventional "reload your config" signal (Stage 7d).
func reloadSignals() []os.Signal { return []os.Signal{syscall.SIGHUP} }
