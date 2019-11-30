package loghive

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack"

	"github.com/dgraph-io/badger"
	"github.com/notduncansmith/mutable"
)

const segmentMetaKey = "_meta"
const segmentFilenamePrefix = "segment-"
const segmentDir = ".data"

// Segment is a file that represents a contiguous subset of logs in a domain starting at a timestamp
type Segment struct {
	Path      string    `json:"path"`
	Domain    string    `json:"domain"`
	Timestamp time.Time `json:"timestamp"`
}

// DBMap is a map of DB paths to open database handles
type DBMap = map[string]*badger.DB

// SegmentMap is a map of Segments by Domain
type SegmentMap = map[string][]Segment

// SegmentManager manages connections to Segments in BadgerDBs
type SegmentManager struct {
	*mutable.RW
	DBMap
	SegmentMap
	Path string
}

// NewSegmentManager creates a new SegmentManager for a specified path
func NewSegmentManager(path string) *SegmentManager {
	return &SegmentManager{mutable.NewRW("SegmentManager"), DBMap{}, SegmentMap{}, filepath.Join(path, segmentDir)}
}

// ScanDir scans the directory at `m.Path` for segment DB files and opens them
func (m *SegmentManager) ScanDir() ([]Segment, error) {
	_, err := os.Stat(m.Path)
	if err != nil {
		fmt.Println("Creating " + m.Path)
		err = os.Mkdir(m.Path, 0600)
	}
	if err != nil {
		return nil, err
	}

	files, err := ioutil.ReadDir(m.Path)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Scanned path %v, found %v files", m.Path, len(files))

	segments := make([]Segment, len(files))
	for i, file := range files {
		fmt.Printf("Reading segment #%v meta from %v", i, file.Name())
		segments[i], err = m.readSegmentMeta(file.Name())
		if err != nil {
			return nil, err
		}
		fmt.Println(segments[i])
	}

	sort.Slice(segments, func(a, b int) bool {
		return segments[a].Timestamp.Before(segments[b].Timestamp)
	})

	for _, s := range segments {
		cur := m.SegmentMap[s.Domain]
		if cur == nil {
			m.SegmentMap[s.Domain] = []Segment{s}
		} else {
			m.SegmentMap[s.Domain] = append(cur, s)
		}
	}

	return segments, err
}

// Write stores a group of logs in their appropriate segment files, reporting any failures on a given channel
func (m *SegmentManager) Write(logs []Log) []LogWriteFailure {
	errs := []LogWriteFailure{}
	mut := mutable.NewRW("errs")

	// first we batch each segment's logs together...
	segmentLogs := map[string][]Log{}
	for _, log := range logs {
		s, err := m.segmentForLog(&log)
		if err != nil {
			errs = append(errs, LogWriteFailure{err, log, 1})
			continue
		}
		if segmentLogs[s.Path] == nil {
			segmentLogs[s.Path] = []Log{}
		}
		segmentLogs[s.Path] = append(segmentLogs[s.Path], log)
	}

	// ...then write each batch
	wg := sync.WaitGroup{}
	for path, logs := range segmentLogs {
		wg.Add(1)
		go func(p string, l []Log) {
			err := m.writeSegmentLogs(p, l)
			if err != nil {
				mut.DoWithRWLock(func() {
					errs = append(errs, LogWriteFailure{err, l[0], len(l)})
				})
			}
			wg.Done()
		}(path, logs)
	}
	wg.Wait()

	return errs
}

// Iterate takes a list of domains and a range of time, returning a list of channels (one per domain) on which chunks of the requested size will be delivered
func (m *SegmentManager) Iterate(domains []string, start, end time.Time, chunkSize int, bufferSize int) []chan []Log {
	chunkChans := make([]chan []Log, len(domains))

	for idx, domain := range domains {
		chunkChans[idx] = make(chan []Log, bufferSize)
		go func(i int, d string) {
			for _, segment := range m.segmentsInRange(d, start, end) {
				go func(s Segment) {
					err := m.segmentChunks(d, s.Path, chunkSize, start, end, chunkChans[i])
					if err != nil {
						log.Println(err)
						return
					}
				}(segment)
			}
		}(idx, domain)
	}

	return chunkChans
}

// CreateSegment creates a BadgerDB file for a segment and stores its metadata in it
func (m *SegmentManager) CreateSegment(domain string, timestamp time.Time) (Segment, error) {
	path := filepath.Join(m.Path, segmentFilenamePrefix+string(timeToBytes(timestamp)))
	segment := Segment{path, domain, timestamp}

	db, err := m.openDB(path)
	if err != nil {
		return Segment{}, err
	}

	bz, err := msgpack.Marshal(segment)
	if err != nil {
		return Segment{}, err
	}

	err = db.Update(func(tx *badger.Txn) error {
		return tx.Set([]byte(segmentMetaKey), bz)
	})

	m.DoWithRWLock(func() {
		cur := m.SegmentMap[segment.Domain]
		if cur == nil {
			m.SegmentMap[segment.Domain] = []Segment{segment}
		} else {
			m.SegmentMap[segment.Domain] = append(cur, segment)
		}
	})

	return segment, err
}

// CreateNeededSegments checks the latest segment in each domain and if any exceed the given constraints, a new segment is created in that domain
func (m *SegmentManager) CreateNeededSegments(maxBytes int64, maxDuration time.Duration) error {
	now := time.Now()
	errs := []error{}

	for domain, segments := range m.SegmentMap {
		latest := segments[len(segments)-1]
		if latest.Timestamp.Add(maxDuration).Before(now) {
			_, err := m.CreateSegment(domain, now)
			if err != nil {
				errs = append(errs)
				continue
			}
		}
		f, err := os.Stat(latest.Path)
		if err != nil {
			errs = append(errs)
			continue
		}
		if f.Size() > maxBytes {
			_, err := m.CreateSegment(domain, now)
			if err != nil {
				errs = append(errs)
				continue
			}
		}
	}

	if len(errs) > 0 {
		return coalesceErrors("Create Needed Segments", errs)
	}

	return nil
}

// readSegmentMeta fetches the metadata stored in a segment DB
func (m *SegmentManager) readSegmentMeta(fileName string) (Segment, error) {
	var segment Segment
	db, err := m.openDB(filepath.Join(m.Path, fileName))
	if err != nil {
		return segment, err
	}
	err = db.View(func(tx *badger.Txn) error {
		item, err := tx.Get([]byte(segmentMetaKey))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &segment)
		})
	})
	return segment, err
}

// segmentForLog ensures that a log can be written to the latest segment in its domain
func (m *SegmentManager) segmentForLog(l *Log) (Segment, error) {
	domainSegments := m.SegmentMap[l.Domain]
	if domainSegments == nil {
		return Segment{}, errInvalidLogDomain(l.Domain)
	}

	segment := domainSegments[len(domainSegments)-1]
	if l.Timestamp.Before(segment.Timestamp) {
		return Segment{}, errUnableToBackfill(l.Domain, l.Timestamp)
	}

	return segment, nil
}

// writeSegmentLogs writes a batch of logs to a segment DB
func (m *SegmentManager) writeSegmentLogs(path string, logs []Log) error {
	db, err := m.openDB(path)
	if err != nil {
		return err
	}
	return db.Update(func(txn *badger.Txn) error {
		var err error
		for _, log := range logs {
			if err = txn.Set(timeToBytes(log.Timestamp), log.Line); err != nil {
				return err
			}
		}
		return nil
	})
}

// segmentChunks delivers chunks of logs of the requested size from the selected segment on the given channel
func (m *SegmentManager) segmentChunks(domain, path string, chunkSize int, start, end time.Time, chunks chan []Log) error {
	db, err := m.openDB(path)
	if err != nil {
		return err
	}
	return db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // key-only iteration
		it := txn.NewIterator(opts)
		bz, err := start.MarshalBinary()
		if err != nil {
			return err
		}
		chunk := make([]Log, chunkSize)
		for it.Seek(bz); it.Valid(); it.Next() {
			keytime := time.Time{}
			kerr := keytime.UnmarshalBinary(it.Item().Key())
			if kerr != nil {
				return err
			}
			if keytime.Before(end) {
				return it.Item().Value(func(val []byte) error {
					chunk = append(chunk, Log{domain, keytime, val})
					if len(chunk) == chunkSize {
						chunks <- chunk
						chunk = make([]Log, chunkSize)
					}
					return nil
				})
			}
		}

		if len(chunk) > 0 {
			chunks <- chunk
		}

		return nil
	})
}

// openDB opens a DB at a path, or returns one that has already been opened
func (m *SegmentManager) openDB(path string) (*badger.DB, error) {
	fullPath := filepath.Join(path)
	var db *badger.DB

	m.DoWithRLock(func() {
		db = m.DBMap[fullPath]
	})

	if db != nil {
		return db, nil
	}

	db, err := badger.Open(badger.DefaultOptions(fullPath))

	if err != nil {
		return nil, errUnreachable(fullPath, err.Error())
	}

	m.DoWithRWLock(func() {
		m.DBMap[fullPath] = db
	})

	return db, nil
}

func (m *SegmentManager) segmentsInRange(domain string, start, end time.Time) []Segment {
	inRange := []Segment{}
	m.DoWithRLock(func() {
		for _, s := range m.SegmentMap[domain] {
			if s.Timestamp.After(start) && s.Timestamp.Before(end) {
				inRange = append(inRange, s)
			}
		}
	})
	return inRange
}

func (m *SegmentManager) segmentsAfter(domain string, start time.Time) []Segment {
	inRange := []Segment{}
	m.DoWithRLock(func() {
		for _, s := range m.SegmentMap[domain] {
			if s.Timestamp.After(start) {
				inRange = append(inRange, s)
			}
		}
	})
	return inRange
}
