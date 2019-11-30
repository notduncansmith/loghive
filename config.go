package loghive

import (
	"time"

	du "github.com/notduncansmith/duramap"
)

// ConfigFilename defines the name of the database file for storing configuration details
const ConfigFilename = "config.db"

// DefaultConfig is the default configuration, which will be written to the config database if a config is not found
var DefaultConfig = Config{du.GenericMap{
	"Debug":              false,
	"WritableDomains":    []string{"_internal"},
	"SegmentMaxDuration": time.Duration(336 * time.Hour), // 336/24=14 days
	"SegmentMaxBytes":    128 * 1024 * 1024,              // 128 MiB
	"LineMaxBytes":       8 * 1024,                       // 8 KiB
}}

// Config describes the configuration that Loghive needs to function
type Config struct {
	m du.GenericMap
}

// Debug gets the `debug` value from the config map
func (c *Config) Debug() bool {
	return c.m["Debug"].(bool)
}

// WritableDomains gets the `WritableDomains` value from the config map
func (c *Config) WritableDomains() []string {
	return c.m["WritableDomains"].([]string)
}

// SegmentMaxDuration gets the `segmentMaxDuration` value from the config map
func (c *Config) SegmentMaxDuration() time.Duration {
	return c.m["SegmentMaxDuration"].(time.Duration)
}

// SegmentMaxBytes gets the `segmentMaxBytes` value from the config map
func (c *Config) SegmentMaxBytes() int64 {
	return c.m["SegmentMaxBytes"].(int64)
}

// LineMaxBytes gets the `lineMaxBytes` value from the config map
func (c *Config) LineMaxBytes() int {
	return c.m["LineMaxBytes"].(int)
}

func setDefaults(m du.GenericMap) du.GenericMap {
	for k, v := range DefaultConfig.m {
		if m[k] == nil {
			m[k] = v
		}
	}
	return m
}
