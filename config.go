package loghive

import (
	"time"
)

// DefaultConfig is the default configuration, which will be written to the config database if a config is not found
var DefaultConfig = Config{
	Debug:              false,
	WritableDomains:    []string{"_"},
	SegmentMaxDuration: time.Duration(336) * time.Hour, // 336/24=14 days
	SegmentMaxBytes:    128 * 1024 * 1024,              // 128 MiB
	LineMaxBytes:       8 * 1024,                       // 8 KiB
	FlushAfterItems:    128,
	FlushAfterDuration: time.Duration(1000) * time.Millisecond,
}

// Config describes the configuration that Loghive needs to function
type Config struct {
	Debug              bool
	WritableDomains    []string
	SegmentMaxDuration time.Duration
	SegmentMaxBytes    int64
	LineMaxBytes       int
	FlushAfterItems    int
	FlushAfterDuration time.Duration
}
