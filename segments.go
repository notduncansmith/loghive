package loghive

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3" // enable sqlite3 support
	"github.com/notduncansmith/mutable"
	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack"
)

const segmentMetaKey = "_meta"
const segmentDirPrefix = "segment-"
const segmentDataFile = "data.db"

// Segment is a file that represents a contiguous subset of logs in a domain starting at a timestamp
type Segment struct {
	Path      string    `json:"path"`
	Domain    string    `json:"domain"`
	Timestamp time.Time `json:"timestamp"`
}

// DBMap is a map of DB paths to open database handles
type DBMap = map[string]*sql.DB

// SegmentMap is a map of Segments by Domain
type SegmentMap = map[string][]Segment

// SegmentManager manages connections to Segments in BadgerDBs
type SegmentManager struct {
	*mutable.RW
	DBMap
	SegmentMap
	Path          string
	PathsToDelete []string
}

// NewSegmentManager creates a new SegmentManager for a specified path
func NewSegmentManager(path string) *SegmentManager {
	return &SegmentManager{mutable.NewRW("SegmentManager"), DBMap{}, SegmentMap{}, path, []string{}}
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
		if !strings.HasPrefix(file.Name(), segmentDirPrefix) {
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
			alreadyLoaded := false
			for _, existing := range m.SegmentMap[s.Domain] {
				if s.Path == existing.Path {
					ic.L().Debugf("Skipping already-loaded segment %v", s.Path)
					alreadyLoaded = true
					break
				}
			}
			if !alreadyLoaded {
				l.Debug("Adding segment to domain")
				m.SegmentMap[s.Domain] = append(cur, s)
			}
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
			defer wg.Done()
			err := m.writeSegmentLogs(p, l)
			if err != nil {
				ic.L().Errorf("Log write failure %v", err)
				mut.DoWithRWLock(func() {
					errs = append(errs, LogWriteFailure{err, *l[0], len(l)})
				})
			} else {
				ic.L().Debugf("Wrote %v logs to %v", len(l), p)
			}
		}(path, pathLogs)
	}
	wg.Wait()

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

// CreateSegment creates a SQLite file for a segment and stores its metadata in it
func (m *SegmentManager) CreateSegment(domain string, timestamp time.Time) (Segment, error) {
	relativePath := segmentDirPrefix + string(timeToBytes(timestamp))
	segment := Segment{relativePath, domain, timestamp}
	err := os.Mkdir(filepath.Join(m.Path, relativePath), 0700)

	db, err := m.openDB(relativePath)
	if err != nil {
		return Segment{}, err
	}

	bz, err := msgpack.Marshal(segment)
	if err != nil {
		return Segment{}, err
	}

	stmt, err := db.Prepare("insert into kv(k, v) values (?, ?)")
	res, err := stmt.Exec(segmentMetaKey, bz)
	if err != nil {
		return segment, err
	}

	id, ierr := res.LastInsertId()
	affected, aerr := res.RowsAffected()
	logrus.Debugf("Result of setting meta key (id: %v): %v %v %v", id, ierr, affected, aerr)

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
		_, err := m.CreateSegment(d, timestamp())
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

		sizedOut, err := segmentSizedOut(m.segmentPath(latest.Path), maxBytes)
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

// MarkSegmentForDeletion will mark a segment to deleted during the next DeleteMarkedSegments
func (m *SegmentManager) MarkSegmentForDeletion(s Segment) error {
	var err error

	m.DoWithRWLock(func() {
		for _, p := range m.PathsToDelete {
			if p == s.Path {
				err = errors.New("Already marked segment for deletion")
				return
			}
		}
		m.PathsToDelete = append(m.PathsToDelete, s.Path)
	})

	return err
}

// DeleteMarkedSegments will delete any segments marked for deletion
func (m *SegmentManager) DeleteMarkedSegments() error {
	errs := []error{}

	for _, p := range m.PathsToDelete {
		if err := m.deleteSegment(p); err != nil {
			errs = append(errs, err)
		}
	}

	return coalesceErrors("delete segments", errs)
}

func (m *SegmentManager) deleteSegment(relativePath string) error {
	var err error
	fullPath := m.segmentPath(relativePath)
	m.DoWithRWLock(func() {
		err = m.DBMap[fullPath].Close()
		if err != nil {
			return
		}
		delete(m.DBMap, fullPath)
		err = os.RemoveAll(filepath.Join(m.Path, relativePath))
	})
	return err
}

// readSegmentMeta fetches the metadata stored in a segment DB
func (m *SegmentManager) readSegmentMeta(relativePath string) (Segment, error) {
	var segment Segment

	db, err := m.openDB(relativePath)
	if err != nil {
		return segment, err
	}

	rows, err := db.Query("select * from kv where id = 1")
	if err != nil {
		return segment, err
	}

	var id int
	var k string
	var v string

	for rows.Next() {
		err = rows.Scan(&id, &k, &v)
		if err != nil {
			return segment, err
		}
		err = msgpack.Unmarshal([]byte(v), &segment)
		break
	}
	if segment.Path == "" {
		return segment, errMalformedSegment(relativePath, errors.New("No metadata available"))
	}
	segment.Timestamp = segment.Timestamp.UTC()

	return segment, err
}

// segmentForLog ensures that a log can be written to the latest segment in its domain
func (m *SegmentManager) segmentForLog(l *Log) (Segment, error) {
	domainSegments := m.SegmentMap[l.Domain]
	segment := domainSegments[len(domainSegments)-1]
	if l.Timestamp.Before(segment.Timestamp) {
		return Segment{}, errUnableToBackfill(l.Domain, segment.Timestamp, l.Timestamp)
	}

	return segment, nil
}

// writeSegmentLogs writes a batch of logs to a segment DB
func (m *SegmentManager) writeSegmentLogs(relativePath string, logs []*Log) error {
	db, err := m.openDB(relativePath)
	if err != nil {
		return err
	}

	valueStrings := make([]string, 0, len(logs))
	valueArgs := make([]interface{}, 0, len(logs)*2)
	for _, log := range logs {
		if string(log.Line) == SyntheticFailureWrite {
			return errors.New("Synthetic write failure")
		}
		valueStrings = append(valueStrings, "(?, ?)")
		valueArgs = append(valueArgs, log.Timestamp.UnixNano())
		valueArgs = append(valueArgs, string(log.Line))
	}
	stmt := fmt.Sprintf("insert into logs(t, l) values %s", strings.Join(valueStrings, ","))
	res, err := db.Exec(stmt, valueArgs...)
	id, ierr := res.LastInsertId()
	affected, aerr := res.RowsAffected()
	logrus.Debugf("Log write result (id: %v): %v %v %v", id, ierr, affected, aerr)
	return err
}

// segmentChunks delivers chunks of logs of the requested size from the selected segment on the given channel
func (m *SegmentManager) segmentChunks(domain, relativePath string, chunkSize int, start, end time.Time, chunks chan []Log) error {
	db, err := m.openDB(relativePath)
	if err != nil {
		return err
	}
	rows, err := db.Query("select * from logs where t >= ? and t <= ?", start.UnixNano(), end.UnixNano())
	if err != nil {
		return err
	}

	var chunk []Log
	var chunkCount int
	var id int
	var timestampInt int64
	var lineStr string

	for rows.Next() {
		err = rows.Scan(&id, &timestampInt, &lineStr)
		if err != nil {
			return err
		}
		logToAdd := Log{domain, time.Unix(0, timestampInt).UTC(), []byte(lineStr)}
		logrus.Debugf("Adding log to chunk %v %v", logToAdd, relativePath)
		chunk = append(chunk, logToAdd)
		if len(chunk) == chunkSize {
			chunkCount++
			logrus.Debugf("Appending chunk %v (count %v) %v", chunk, chunkCount, relativePath)
			chunks <- chunk
			chunk = []Log{}
		}
	}

	if len(chunk) > 0 {
		logrus.Infof("Leftover chunk values, chunking (%v) %v", chunk, relativePath)
		chunks <- chunk
	}

	err = rows.Err()
	if err != nil {
		rows.Close()
		return err
	}

	return rows.Close()
}

// openDB opens a DB at a path, or returns one that has already been opened
func (m *SegmentManager) openDB(relativePath string) (*sql.DB, error) {
	var db *sql.DB
	fullPath := m.segmentPath(relativePath)

	m.DoWithRLock(func() {
		db = m.DBMap[fullPath]
	})

	if db != nil {
		return db, nil
	}

	db, err := sql.Open("sqlite3", fullPath)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Opened db %v", db)

	stmts := []string{
		`pragma journal_mode = WAL;`,
		`create table if not exists logs(
			id integer primary key,
			t integer,
			l text
		);`,
		`create table if not exists kv(
			id integer primary key,
			k text,
			v blob
		);`,
	}

	for _, s := range stmts {
		stmt, err := db.Prepare(s)
		if err != nil {
			logrus.Debugf("Error loading schema: %v %v", s, stmt)
			return nil, err
		}

		res, err := stmt.Exec()
		if err != nil {
			logrus.Debugf("Error loading schema: %v %v", s, res)
			return nil, err
		}
		logrus.Debugf("Result: %v", res)
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

func (m *SegmentManager) segmentPath(relativePath string) string {
	return filepath.Join(m.Path, relativePath, segmentDataFile)
}

func segmentAgedOut(s Segment, now time.Time, maxDuration time.Duration) bool {
	return s.Timestamp.Add(maxDuration).Before(now)
}

func segmentSizedOut(path string, maxBytes int64) (bool, error) {
	size, err := segmentSize(path)
	return size > maxBytes, err
}

func segmentSize(path string) (int64, error) {
	f, err := os.Stat(path)
	if err != nil {
		return 0, err
	}

	f2, err := os.Stat(path + "-journal")
	if err != nil {
		return 0, err
	}

	return f.Size() + f2.Size(), nil
}
