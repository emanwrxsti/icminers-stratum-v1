// Package logging provides a thin wrapper over log/slog so that every
// component and every pool/coin can carry independent, structured labels.
//
// The isolation spec requires that "each pool must have independent logs and
// metrics labels". We satisfy that here by handing each pool a child logger
// created with For(poolID); all of that pool's log lines then carry a stable
// pool= attribute without the call sites needing to remember to add it.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// Logger is an alias so the rest of the codebase does not import slog directly
// and we retain the freedom to swap the backend later.
type Logger = slog.Logger

// Options controls logger construction.
type Options struct {
	// Level is one of debug, info, warn, error (case-insensitive).
	Level string
	// JSON selects JSON output when true, otherwise a human-readable text
	// handler is used (useful for local development).
	JSON bool
}

// New builds a root logger from the given options.
func New(opts Options) *Logger {
	lvl := parseLevel(opts.Level)
	handlerOpts := &slog.HandlerOptions{Level: lvl}

	var h slog.Handler
	if opts.JSON {
		h = slog.NewJSONHandler(os.Stdout, handlerOpts)
	} else {
		h = slog.NewTextHandler(os.Stdout, handlerOpts)
	}
	return slog.New(h)
}

// Component returns a child logger tagged with component=<name>.
func Component(l *Logger, name string) *Logger {
	return l.With(slog.String("component", name))
}

// ForPool returns a child logger tagged with pool=<poolID>. This is the primary
// mechanism for per-pool log isolation.
func ForPool(l *Logger, poolID string) *Logger {
	return l.With(slog.String("pool", poolID))
}

// ForRegion returns a child logger tagged with region=<region>.
func ForRegion(l *Logger, region string) *Logger {
	return l.With(slog.String("region", region))
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "", "info":
		return slog.LevelInfo
	default:
		return slog.LevelInfo
	}
}
