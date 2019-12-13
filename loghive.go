package loghive

import (
	"errors"

	"github.com/notduncansmith/bbq"
	"github.com/notduncansmith/mutable"
	"github.com/sirupsen/logrus"
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
	h.setupInternalLogging()
	h.incoming = bbq.NewBatchQueue(h.flush, bbq.BatchQueueOptions{
		FlushTime:  config.FlushAfterDuration,
		FlushCount: config.FlushAfterItems,
	})
	segments, err := sm.ScanDir()
	if err != nil {
		return nil, err
	}
	for _, d := range config.WritableDomains {
		if sm.SegmentMap[d] == nil {
			s, err := sm.CreateSegment(d, timestamp())
			if err != nil {
				return nil, err
			}
			logrus.Infof("Created segment %v for writable domain %v", s.Path, d)
			segments = append(segments, s)
		}
	}
	logrus.Infof("Loaded %v segment(s)", len(segments))
	return h, nil
}

// Enqueue constructs a Log from the given domain and line, and puts it on the queue to be written
func (h *Hive) Enqueue(domain string, line []byte) (bbq.Callback, error) {
	if !h.domainValid(domain) {
		return nil, errInvalidLogDomain(domain)
	}
	if line == nil {
		return nil, errLineMissing()
	}
	if tooLong := h.lineTooLong(len(line)); tooLong {
		return nil, errLineTooLarge(len(line), h.config.LineMaxBytes)
	}
	log := NewLog(domain, line)
	logrus.Debugf("Enqueing log %v", log)
	return h.incoming.Enqueue(log), nil
}

// FailToLogThisLine is an ugly hack that exists because I can't figure out how to force a write failure during tests.
const FailToLogThisLine = "!~!~!~_internal_::thislogshouldfail👎"

// flush converts bbq interface{} items to *Logs and writes them, then creates any needed segments
func (h *Hive) flush(items []interface{}) error {
	logs := []*Log{}
	for _, i := range items {
		log, _ := i.(*Log)
		logs = append(logs, log)
	}
	logrus.Debugf("Flushing %v logs", len(logs))
	err := h.sm.Write(logs)
	if err != nil {
		logrus.Errorf("Error flushing logs %v", err)
		return err
	}

	err = h.sm.CreateNeededSegments(h.config.SegmentMaxBytes, h.config.SegmentMaxDuration)
	if err != nil {
		logrus.Errorf("Error creating new segments %v", err)
		return err
	}

	// warning: ugly hack
	if h.config.InternalLogLevel == logrus.DebugLevel {
		if string(logs[0].Line) == FailToLogThisLine {
			return errors.New("Synthetic flush failure")
		}
	}

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

func (h *Hive) setupInternalLogging() {
	if h.config.InternalLogFormat == LogFormatJSON {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	} else {
		logrus.SetFormatter(&logrus.TextFormatter{})
	}

	logrus.SetLevel(h.config.InternalLogLevel)
}
