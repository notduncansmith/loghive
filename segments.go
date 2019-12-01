package loghive

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/notduncansmith/mutable"
	"github.com/vmihailenco/msgpack"
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

// Close closes all managed DBs
func (m *SegmentManager) Close() {
	for _, db := range m.DBMap {
		db.Close()
	}
}

// ScanDir scans the directory at `m.Path` for segment DB files and opens them
func (m *SegmentManager) ScanDir() ([]Segment, error) {
	_, err := os.Stat(m.Path)
	if err != nil {
		fmt.Println("Creating " + m.Path)
		err = os.MkdirAll(m.Path, 0700)
	}
	if err != nil {
		return nil, err
	}

	files, err := ioutil.ReadDir(m.Path)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Scanned path %v, found %v files\n", m.Path, len(files))

	segments := make([]Segment, len(files))
	for i, file := range files {
		if !strings.HasPrefix(file.Name(), segmentFilenamePrefix) {
			fmt.Printf("Skipping non-segment file %v\n", file.Name())
			continue
		}
		fmt.Printf("Reading segment #%v meta from %v\n", i, file.Name())
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

// Write stores a group of logs in their appropriate segment files, returning any failures
func (m *SegmentManager) Write(logs []*Log) []LogWriteFailure {
	errs := []LogWriteFailure{}
	mut := mutable.NewRW("errs")

	// first we batch each segment's logs together...
	segmentLogs := map[string][]*Log{}
	for _, log := range logs {
		s, err := m.segmentForLog(log)
		if err != nil {
			errs = append(errs, LogWriteFailure{err, *log, 1})
			continue
		}
		if segmentLogs[s.Path] == nil {
			segmentLogs[s.Path] = []*Log{}
		}
		segmentLogs[s.Path] = append(segmentLogs[s.Path], log)
	}

	fmt.Println("Segment batches to write", segmentLogs, errs)

	// ...then write each batch
	wg := sync.WaitGroup{}
	for path, logs := range segmentLogs {
		wg.Add(1)
		fmt.Println("Writing batch at path " + path)
		go func(p string, l []*Log) {
			err := m.writeSegmentLogs(p, l)
			if err != nil {
				fmt.Println("Log write failure", err)
				mut.DoWithRWLock(func() {
					errs = append(errs, LogWriteFailure{err, *l[0], len(l)})
				})
			}
			wg.Done()
		}(path, logs)
	}
	wg.Wait()
	fmt.Printf("Wrote %v logs\n", len(logs))

	return errs
}

// Iterate takes a list of domains and a range of time, returning a list of channels (one per domain) on which chunks of the requested size will be delivered
func (m *SegmentManager) Iterate(domains []string, start, end time.Time, chunkSize int, bufferSize int) []chan []Log {
	chunkChans := make([]chan []Log, len(domains))

	for idx, domain := range domains {
		chunkChans[idx] = make(chan []Log, bufferSize)
		go func(i int, d string) {
			defer close(chunkChans[i])
			segments := m.segmentsInRange(d, start, end)
			fmt.Printf("Segments in range: %v\n", segments)
			for _, segment := range segments {
				fmt.Printf("Iterating segment %v @ %v\n", segment.Domain, segment.Path)
				err := m.segmentChunks(d, segment.Path, chunkSize, start, end, chunkChans[i])
				if err != nil {
					log.Println(err)
					return
				}
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
			fmt.Printf("Segment %v too old, creating new segment\n", latest.Path)
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
			fmt.Printf("Segment %v too large, creating new segment\n", latest.Path)
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
		fmt.Println("Invalid domain", *l)
		return Segment{}, errInvalidLogDomain(l.Domain)
	}

	segment := domainSegments[len(domainSegments)-1]
	if l.Timestamp.Before(segment.Timestamp) {
		return Segment{}, errUnableToBackfill(l.Domain, l.Timestamp)
	}

	return segment, nil
}

// writeSegmentLogs writes a batch of logs to a segment DB
func (m *SegmentManager) writeSegmentLogs(path string, logs []*Log) error {
	db, err := m.openDB(path)
	if err != nil {
		return err
	}
	return db.Update(func(tx *badger.Txn) error {
		var err error
		for _, log := range logs {
			if err = tx.Set(timeToBytes(log.Timestamp), log.Line); err != nil {
				return err
			}
		}
		fmt.Println("Finished writing logs", logs)
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
		defer it.Close()
		chunk := []Log{}
		fmt.Println("IT: " + string(timeToBytes(start)))
		for it.Seek(timeToBytes(start)); it.Valid(); it.Next() {
			fmt.Printf("Chunk size: %v\n", len(chunk))
			kbz := it.Item().Key()
			fmt.Println("IT: " + string(kbz))
			if string(kbz) == segmentMetaKey {
				continue
			}
			keytime := time.Time{}
			kerr := keytime.UnmarshalText(kbz)
			if kerr != nil {
				fmt.Printf("Error unmarshaling keytime %v\n", kerr)
				return err
			}
			fmt.Printf("Iterator at keytime %v\n", keytime)
			if keytime.Before(end) {
				fmt.Println("In range")
				it.Item().Value(func(val []byte) error {
					chunk = append(chunk, Log{domain, keytime, val})
					if len(chunk) == chunkSize {
						fmt.Println("Chunking", chunk)
						chunks <- chunk
						chunk = []Log{}
					}
					return nil
				})
			} else {
				fmt.Println("End of range")
				break
			}
		}

		if len(chunk) > 0 {
			fmt.Printf("Leftover chunk values, chunking (%v)", len(chunk))
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
