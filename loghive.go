package loghive

import (
	"fmt"
	"path/filepath"

	"github.com/notduncansmith/bbq"
	du "github.com/notduncansmith/duramap"
	"github.com/notduncansmith/mutable"
)

// Hive is a running Loghive instance pointed at a storage path
type Hive struct {
	*mutable.RW
	sm          *SegmentManager
	StoragePath string
	config      *du.Duramap
	incoming    *bbq.BatchQueue
}

// NewHive returns a pointer to a Hive at the given path. Configuration will be loaded from a file found there or populated from defaults.
func NewHive(path string) (*Hive, error) {
	sm := NewSegmentManager(path)
	mut := mutable.NewRW("Hive[" + path + "]")
	h := &Hive{mut, sm, path, nil, nil}
	h.incoming = bbq.NewBatchQueue(h.flush, bbq.DefaultOptions)
	config, err := h.loadConfig()
	fmt.Printf("Loaded config: %v\n", config)
	if err != nil {
		return nil, err
	}
	h.config = config
	segments, err := sm.ScanDir()
	if err != nil {
		return nil, err
	}
	fmt.Printf("Loaded %v segments\n", len(segments))
	return h, nil
}

// Enqueue constructs a Log from the given domain and line, and puts it on the queue to be written
func (h *Hive) Enqueue(domain string, line []byte) (bbq.Callback, error) {
	if !h.domainValid(domain) {
		return nil, errInvalidLogDomain(domain)
	}

	if tooLong, max := h.lineTooLong(len(line)); tooLong {
		return nil, errLineTooLarge(len(line), max)
	}

	return h.incoming.Enqueue(NewLog(domain, line)), nil
}

// DoWithConfig acquires a read lock on the config and calls f with it
func (h *Hive) DoWithConfig(f func(Config)) {
	h.config.DoWithMap(func(m du.GenericMap) {
		f(Config{m})
	})
}

// UpdateConfig acquires a read-write lock on the config and calls f with it
func (h *Hive) UpdateConfig(f func(Config) Config) error {
	return h.config.UpdateMap(func(m du.GenericMap) du.GenericMap {
		return f(Config{m}).m
	})
}

// flush converts bbq interface{} items to *Logs and writes them, then creates any needed segments
func (h *Hive) flush(items []interface{}) error {
	logs := []Log{}
	for _, i := range items {
		log, _ := i.(Log)
		logs = append(logs, log)
	}
	errs := h.sm.Write(logs)
	if len(errs) > 0 {
		return coalesceLogWriteFailures(errs)
	}

	h.DoWithConfig(func(c Config) {
		h.sm.CreateNeededSegments(c.SegmentMaxBytes(), c.SegmentMaxDuration())
	})

	return nil
}

func (h *Hive) loadConfig() (*du.Duramap, error) {
	path := filepath.Join(h.StoragePath, ConfigFilename)
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

func (h *Hive) domainValid(domain string) bool {
	writable := false
	h.DoWithConfig(func(c Config) {
		for _, d := range c.WritableDomains() {
			if d == domain {
				writable = true
			}
		}
	})
	return writable
}

func (h *Hive) lineTooLong(l int) (bool, int) {
	tooLong := false
	max := 0
	h.DoWithConfig(func(c Config) {
		max = c.LineMaxBytes()
		tooLong = l > max
	})
	return tooLong, max
}
