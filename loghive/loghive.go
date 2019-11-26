package loghive

import (
	"path/filepath"

	"github.com/notduncansmith/bbq"
	du "github.com/notduncansmith/duramap"
	"github.com/notduncansmith/mutable"
)

// Hive is a running Loghive process pointed at a storage path
type Hive struct {
	*mutable.RW
	*DBManager
	StoragePath    string
	config         *du.Duramap
	incoming       *bbq.BatchQueue
	DomainSegments Segments `json:"segments"`
}

// NewHive returns a pointer to a Hive with the given configuration
func NewHive(path string) (*Hive, error) {
	dbm := NewDBManager(path)
	mut := mutable.NewRW("Hive[" + path + "]")
	h := &Hive{mut, dbm, path, nil, nil, Segments{}}
	h.incoming = bbq.NewBatchQueue(h.flush, bbq.DefaultOptions)
	config, err := h.loadConfig()
	if err != nil {
		return nil, err
	}
	h.config = config
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
func (h *Hive) UpdateConfig(f func(Config) Config) {
	h.config.UpdateMap(func(m du.GenericMap) du.GenericMap {
		return f(Config{m}).m
	})
}

// flush converts bbq interface{} items to *Logs and writes them
func (h *Hive) flush(items []interface{}) error {
	logs := []*Log{}
	for _, i := range items {
		log, _ := i.(Log)
		logs = append(logs, &log)
	}
	return h.write(logs)
}

// write appends logs to segments
func (h *Hive) write(logs []*Log) error {
	batches := map[string][]*Log{}

	for _, l := range logs {
		batches[l.Domain] = append(batches[l.Domain], l)
	}

	// TODO: write with SegmentManager

	return nil
}

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
