//go:build windows

package cli

import "os"

// reloadSignals returns no signals on Windows: SIGHUP does not exist there, so
// live config reload is unavailable and picking up config changes requires a
// full process restart. This is an accepted platform limitation (Stage 7d) — a
// local single-user tool is fully usable without SIGHUP reload on Windows.
func reloadSignals() []os.Signal { return nil }
