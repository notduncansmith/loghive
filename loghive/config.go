package loghive

import (
	"path/filepath"
	"time"

	du "github.com/notduncansmith/duramap"
)

// ConfigFilename defines the name of the database file for storing configuration details
const ConfigFilename = "config.db"

// Config describes the configuration that Loghive needs to function
type Config struct {
	m du.GenericMap
}

// Debug gets the `debug` value from the config map
func (c *Config) Debug() bool {
	return c.m["Debug"].(bool)
}

// Domains gets the `domains` value from the config map
func (c *Config) Domains() []string {
	return c.m["Domains"].([]string)
}

// SegmentMaxDuration gets the `segmentMaxDuration` value from the config map
func (c *Config) SegmentMaxDuration() time.Duration {
	return c.m["SegmentMaxDuration"].(time.Duration)
}

// SegmentMaxBytes gets the `segmentMaxBytes` value from the config map
func (c *Config) SegmentMaxBytes() int {
	return c.m["SegmentMaxBytes"].(int)
}

// LineMaxBytes gets the `lineMaxBytes` value from the config map
func (c *Config) LineMaxBytes() int {
	return c.m["LineMaxBytes"].(int)
}

// QueryMaxExpressionLength gets the `queryMaxExpressionLength` value from the config map
func (c *Config) QueryMaxExpressionLength() int {
	return c.m["QueryMaxExpressionLength"].(int)
}

// DefaultConfig is the default configuration, which will be written to the config database if a config is not found
var DefaultConfig = Config{du.GenericMap{
	"Debug":                    false,
	"Domains":                  []string{"_internal"},
	"SegmentMaxDuration":       time.Duration(336 * time.Hour),
	"SegmentMaxBytes":          1024 * 1024 * 128,
	"LineMaxBytes":             1024 * 8,
	"QueryMaxExpressionLength": 1024,
}}

func (h *Hive) loadConfig() (*du.Duramap, error) {
	path := filepath.Join(h.Path, ConfigFilename)
	dm, err := du.NewDuramap(path, "config")
	if err != nil {
		return nil, errUnableToLoadConfig(errUnreachable(path, err.Error()).Error())
	}

	if err = dm.Load(); err != nil {
		return nil, errUnableToLoadConfig(err.Error())
	}

	if err = dm.UpdateMap(setDefaults); err != nil {
		return nil, errUnableToLoadConfig(err.Error())
	}

	return dm, nil
}

// DoWithConfig acquires a read lock on the config and calls f with it
func (h *Hive) DoWithConfig(f func(Config)) {
	h.configDM.DoWithMap(func(m du.GenericMap) {
		f(Config{m})
	})
}

// UpdateConfig acquires a read-write lock on the config and calls f with it
func (h *Hive) UpdateConfig(f func(Config) Config) {
	h.configDM.UpdateMap(func(m du.GenericMap) du.GenericMap {
		return f(Config{m}).m
	})
}

func setDefaults(m du.GenericMap) du.GenericMap {
	for k, v := range DefaultConfig.m {
		if m[k] == nil {
			m[k] = v
		}
	}
	return m
}
