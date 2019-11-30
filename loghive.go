package loghive

import (
	"fmt"

	"github.com/notduncansmith/bbq"
	"github.com/notduncansmith/mutable"
)

// Hive is a running Loghive instance pointed at a storage path
type Hive struct {
	*mutable.RW
	sm          *SegmentManager
	StoragePath string
	config      Config
	incoming    *bbq.BatchQueue
}

// NewHive returns a pointer to a Hive at the given path. Configuration will be loaded from a file found there or populated from defaults.
func NewHive(path string, config Config) (*Hive, error) {
	sm := NewSegmentManager(path)
	mut := mutable.NewRW("Hive[" + path + "]")
	h := &Hive{mut, sm, path, config, nil}
	h.incoming = bbq.NewBatchQueue(h.flush, bbq.BatchQueueOptions{
		FlushTime:  config.FlushAfterDuration,
		FlushCount: config.FlushAfterItems,
	})
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

	if tooLong := h.lineTooLong(len(line)); tooLong {
		return nil, errLineTooLarge(len(line), h.config.LineMaxBytes)
	}

	return h.incoming.Enqueue(NewLog(domain, line)), nil
}

// DoWithConfig acquires a read lock on the config and calls f with it
func (h *Hive) DoWithConfig(f func(Config)) {
	f(h.config)
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

	h.sm.CreateNeededSegments(h.config.SegmentMaxBytes, h.config.SegmentMaxDuration)

	return nil
}

func (h *Hive) domainValid(domain string) bool {
	writable := false
	for _, d := range h.config.WritableDomains {
		if d == domain {
			writable = true
		}
	}
	return writable
}

func (h *Hive) lineTooLong(l int) bool {
	return l > h.config.LineMaxBytes
}
