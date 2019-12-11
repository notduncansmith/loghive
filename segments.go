package loghive

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/notduncansmith/mutable"
	"github.com/sirupsen/logrus"
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
	ic := newIC("SegmentManager.ScanDir")

	_, err := os.Stat(m.Path)
	if err != nil {
		ic.L().Debugf("Creating %v", m.Path)
		err = os.MkdirAll(m.Path, 0700)
	}
	if err != nil {
		return nil, errUnreachable(m.Path, err.Error())
	}

	files, _ := ioutil.ReadDir(m.Path)
	ic.L().Infof("Scanned path %v, found %v files", m.Path, len(files))

	segments := []Segment{}
	for i, file := range files {
		if !strings.HasPrefix(file.Name(), segmentFilenamePrefix) {
			ic.L().Debugf("Skipping non-segment file %v", file.Name())
			continue
		}
		ic.L().Infof("Reading segment #%v meta from %v", i, file.Name())
		segment, err := m.readSegmentMeta(file.Name())
		if err != nil {
			return nil, errMalformedSegment(m.Path, err)
		}
		segments = append(segments, segment)
	}

	sort.Slice(segments, func(a, b int) bool {
		return segments[a].Timestamp.Before(segments[b].Timestamp)
	})

	for _, s := range segments {
		cur := m.SegmentMap[s.Domain]
		l := ic.withSegment(&s).L()
		if cur == nil {
			l.Debug("Initializing domain")
			m.SegmentMap[s.Domain] = []Segment{s}
		} else {
			l.Debug("Adding segment to domain")
			m.SegmentMap[s.Domain] = append(cur, s)
		}
	}

	return segments, err
}

// Write stores a group of logs in their appropriate segment files, returning any failures
func (m *SegmentManager) Write(logs []*Log) error {
	ic := newIC("SegmentManager.Write")
	errs := []LogWriteFailure{}
	mut := mutable.NewRW("errs")

	// first we batch each segment's logs together...
	segmentLogs := map[string][]*Log{}
	for _, log := range logs {
		ic.withLog(log).L().Debug("Assigning log segment for writing")
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

	if len(errs) > 0 {
		ic.L().Warnf("Errors assigning logs to segments: %v", errs)
	}

	ic.L().Debugf("Segment batches to write: %v", segmentLogs)

	// ...then write each batch
	wg := sync.WaitGroup{}
	for path, pathLogs := range segmentLogs {
		wg.Add(1)
		ic.L().Debugf("Writing batch at path %v", path)
		go func(p string, l []*Log) {
			err := m.writeSegmentLogs(p, l)
			if err != nil {
				ic.L().Errorf("Log write failure %v", err)
				mut.DoWithRWLock(func() {
					errs = append(errs, LogWriteFailure{err, *l[0], len(l)})
				})
			} else {
				ic.L().Debugf("Wrote %v logs to %v", len(l), p)
			}
			wg.Done()
		}(path, pathLogs)
	}
	wg.Wait()
	ic.L().Debugf("Wrote %v logs", len(logs))

	return coalesceLogWriteFailures(errs)
}

// Iterate takes a list of domains and a range of time, returning a list of channels (one per domain) on which chunks of the requested size will be delivered
func (m *SegmentManager) Iterate(domains []string, start, end time.Time, chunkSize int, bufferSize int) []chan []Log {
	chunkChans := make([]chan []Log, len(domains))

	for idx, domain := range domains {
		chunkChans[idx] = make(chan []Log, bufferSize)
		go func(i int, d string) {
			defer close(chunkChans[i])
			segments := m.segmentsInRange(d, start, end)
			logrus.Debugf("Segments in range: %v", segments)
			for _, segment := range segments {
				logrus.Debugf("Iterating segment %v @ %v", segment.Domain, segment.Path)
				err := m.segmentChunks(d, segment.Path, chunkSize, start, end, chunkChans[i])
				if err != nil {
					logrus.Println(err)
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
	now := timestamp()
	errs := []error{}

	createFor := func(d string) {
		_, err := m.CreateSegment(d, now)
		if err != nil {
			errs = append(errs)
		}
	}

	for domain, segments := range m.SegmentMap {
		latest := segments[len(segments)-1]

		if segmentAgedOut(latest, now, maxDuration) {
			logrus.Infof("Segment %v retired (age), creating new segment", latest.Path)
			createFor(domain)
			continue
		}

		sizedOut, err := segmentSizedOut(latest, maxBytes)
		if err != nil {
			errs = append(errs)
			continue
		}
		if sizedOut {
			logrus.Infof("Segment %v retired (size), creating new segment", latest.Path)
			createFor(domain)
		}
	}

	return coalesceErrors("Create Needed Segments", errs)
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
		logrus.Debugf("Finished writing logs %v", logs)
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
		var chunk []Log
		for it.Seek(timeToBytes(start)); it.Valid(); it.Next() {
			logrus.Debugf("Chunk size: %v", len(chunk))
			kbz := it.Item().Key()
			logrus.Debugf("Iterator at: %v", string(kbz))
			if string(kbz) == segmentMetaKey {
				continue
			}
			keytime := time.Time{}
			kerr := keytime.UnmarshalText(kbz)
			if kerr != nil {
				logrus.Warnf("Error unmarshaling keytime %v", kerr)
				continue
			}
			logrus.Debugf("Iterator at keytime %v", keytime)
			if keytime.Before(end) {
				logrus.Debug("In range")
				it.Item().Value(func(val []byte) error {
					logVal := make([]byte, len(val))
					copy(logVal, val)
					logToAdd := Log{domain, keytime, logVal}
					logrus.Debugf("Adding log to chunk %v", logToAdd)
					chunk = append(chunk, logToAdd)
					logrus.Debugf("New chunk %v", chunk)
					if len(chunk) == chunkSize {
						logrus.Debugf("Appending chunk %v", chunk)
						chunks <- chunk
						chunk = []Log{}
					}
					return nil
				})
			} else {
				logrus.Debug("End of range")
				break
			}
		}

		if len(chunk) > 0 {
			logrus.Infof("Leftover chunk values, chunking (%v)", chunk)
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
	segments := m.SegmentMap[domain]
	inRange := []Segment{}
	rangeBegan := false
	m.DoWithRLock(func() {
		for i, s := range segments {
			if s.Timestamp.After(start) && !rangeBegan && s.Timestamp.Before(end) {
				rangeBegan = true
				if i > 0 && len(segments) > 1 {
					inRange = append(inRange, segments[i-1])
				}
				inRange = append(inRange, s)
			} else if s.Timestamp.Before(end) {
				inRange = append(inRange, s)
			}
		}
	})
	return inRange
}

func segmentAgedOut(s Segment, now time.Time, maxDuration time.Duration) bool {
	return s.Timestamp.Add(maxDuration).Before(now)
}

func segmentSizedOut(s Segment, maxBytes int64) (bool, error) {
	f, err := os.Stat(s.Path)
	if err != nil {
		return false, err
	}

	return f.Size() > maxBytes, nil
}
