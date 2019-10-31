package loghive

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger"
)

// Segment is a file that represents a contiguous subset of logs in a domain
type Segment struct {
	Path      string    `json:"path"`
	Domain    string    `json:"domain"`
	Timestamp time.Time `json:"timestamp"`
	DB        *badger.DB
}

// Segments is a map of slices of Segments by domain
type Segments map[string][]*Segment

const segmentFilePrefix = "segment--"

// Hive is a running Loghive process pointed at a storage path
type Hive struct {
	*Mutable
	Config         `json:"config"`
	DomainSegments Segments `json:"segments"`
	Started        bool     `json:"started"`
	Queue          chan Log
	Tails          []*Query
}

// NewHive returns a pointer to a Hive with the given configuration
func NewHive(config Config) *Hive {
	q := make(chan Log, config.QueueSize)
	mut := NewMutable("Hive[" + strings.Join(config.Domains, ",") + "]")
	return &Hive{mut, config, Segments{}, false, q, []*Query{}}
}

// Start opens any Segment files at the configured path, and begins accepting writes and queries
func (h *Hive) Start() error {
	h.debug("Starting with domains: %v", h.Domains)
	for _, domain := range h.Domains {
		h.initDomain(domain)
	}

	err := h.startWriting()
	if err != nil {
		return err
	}

	h.Started = true
	return nil
}

// ValidateQuery will return any error with a query's parameters
func (h *Hive) ValidateQuery(query Query) error {
	for _, d := range query.Domains {
		if !h.domainAllowed(d) {
			return errInvalidQuery("invalid domain: " + d)
		}
	}

	if query.Start.Before(query.Start) {
		return errInvalidQuery("start " + query.Start.String() + " before end " + query.End.String())
	}

	return nil
}

// Query reads logs matching a query from one or more databases
func (h *Hive) Query(query Query, queueSize int) error {
	if !h.Started {
		return errNotStarted()
	}

	if err := h.ValidateQuery(query); err != nil {
		return err
	}

	segments := []*Segment{}

	if query.End.Equal(time.Time{}) {
		for _, d := range query.Domains {
			segments = append(segments, h.segmentsAfter(d, query.Start)...)
		}
	} else {
		for _, d := range query.Domains {
			segments = append(segments, h.segmentsInRange(d, query.Start, query.End)...)
		}
	}

	sort.Slice(segments, func(a, b int) bool {
		return segments[a].Timestamp.Before(segments[b].Timestamp)
	})

	linesInRange := make(chan *Log, 1024)
	if queueSize > h.QueryMaxQueueSize {
		queueSize = h.QueryMaxQueueSize
	}

	query.Results = make(chan *Log, queueSize)

	multiread(segments, linesInRange, query.Start, query.End)

	for result := range linesInRange {
		if query.Filter(result) {
			query.Results <- result
		}
	}

	return nil
}

// Tail continuously reads logs matching a query from one or more databases
func (h *Hive) Tail(query Query, queueSize int) error {
	if !h.Started {
		return errNotStarted()
	}

	if queueSize > h.QueryMaxQueueSize {
		queueSize = h.QueryMaxQueueSize
	}

	query.Results = make(chan *Log, queueSize)
	h.Tails = append(h.Tails, &query)

	return nil
}

// Enqueue constructs a Log with timestamp `time.Now()` from the given domain and line, and puts it on the Queue channel to be written
func (h *Hive) Enqueue(domain string, line []byte) error {
	if !h.Started {
		return errNotStarted()
	}

	h.Queue <- Log{domain, timestampNowSeconds(), line}
	return nil
}

// DomainPath constructs the full path to the directory for a Domain
func (h *Hive) DomainPath(domain string) string {
	return fmt.Sprintf("%v/%v", h.StoragePath, domain)
}

// SegmentPath constructs the full path to the file for a Segment
func (h *Hive) SegmentPath(domain string, timestamp time.Time) string {
	timestampSeconds := timestamp.Round(0)
	return fmt.Sprintf("%v/%v/%v%v", h.StoragePath, segmentFilePrefix, domain, timestampSeconds)
}

// initDomains scans and indexes any domain segments within the allowed domains, creating domain directories where not found
func (h *Hive) initDomain(domain string) error {
	h.debug("Initializing Domain %v", domain)
	if h.DomainSegments[domain] == nil {
		h.DomainSegments[domain] = []*Segment{}
	}

	dp := h.DomainPath(domain)
	segmentFileInfos, err := ioutil.ReadDir(dp)
	if err != nil {
		return errDirUnreachable(dp, err.Error())
	}

	for _, fileInfo := range segmentFileInfos {
		segment, err := h.segmentFromFile(domain, fileInfo)

		if err != nil {
			return err
		}

		h.DomainSegments[domain] = append(h.DomainSegments[domain], segment)
	}

	sort.Slice(h.DomainSegments[domain], func(a, b int) bool {
		segmentA := h.DomainSegments[domain][a]
		segmentB := h.DomainSegments[domain][b]
		return segmentA.Timestamp.Before(segmentB.Timestamp)
	})

	if len(h.DomainSegments[domain]) == 0 {
		timestamp := timestampNowSeconds()
		h.debug("Domain %v empty, creating segment at timestamp %v", domain, timeToString(timestamp))
		created, err := h.createSegment(domain, timestamp)
		if err != nil {
			return err
		}
		h.DomainSegments[domain] = append(h.DomainSegments[domain], created)
	}

	return nil
}

// openSegment opens a segment database at a path
func (h *Hive) openSegment(path string) (*badger.DB, error) {
	h.debug("Opening Segment at path %v", path)
	db, err := badger.Open(badger.DefaultOptions(path))

	if err != nil {
		return nil, errUnableToOpenSegment(path, err.Error())
	}

	return db, nil
}

// startWriting spins up a Goroutine to start consuming logs from the queue
func (h *Hive) startWriting() error {
	var err error
	for l := range h.Queue {
		err = h.write(&l)
		if err != nil {
			h.debug("Write error: %v", err.Error())
			return err
		}
	}

	h.debug("Queue channel closed")
	return nil
}

// write actually writes to the database
func (h *Hive) write(l *Log) error {
	lineLen := len(l.Line)
	if lineLen > h.LineMaxBytes {
		return errLineTooLarge(lineLen, h.LineMaxBytes)
	}

	segment, err := h.segmentForLog(l)
	if err != nil {
		return err
	}

	return segment.DB.Update(func(tx *badger.Txn) error {
		return tx.Set(encodeTime(l.Timestamp), l.Line)
	})
}

func (h *Hive) segmentFromFile(domain string, fi os.FileInfo) (*Segment, error) {
	h.debug("Loading segment from file (domain: %v, fileInfo: %v)", domain, fi)
	timestamp, err := timestampFromSegmentFileName(fi.Name())
	if err != nil {
		return nil, errInvalidSegmentFilename(fi.Name())
	}
	return h.createSegment(domain, timestamp)
}

func (h *Hive) createSegment(domain string, timestamp time.Time) (*Segment, error) {
	h.debug("Creating segment (domain: %v, timestamp: %v)", domain, timeToString(timestamp))
	sp := h.SegmentPath(domain, timestamp)
	db, err := h.openSegment(sp)
	if err != nil {
		return nil, err
	}
	return &Segment{sp, domain, timestamp, db}, nil
}

func (h *Hive) segmentForLog(l *Log) (*Segment, error) {
	domain := h.DomainSegments[l.Domain]
	if domain == nil {
		return nil, errInvalidLogDomain(l.Domain)
	}

	segment := domain[0]
	if l.Timestamp.Before(segment.Timestamp) {
		return nil, errUnableToBackfill(l.Domain, l.Timestamp)
	}

	if l.Timestamp.After(segment.Timestamp.Add(h.SegmentMaxDuration)) {
		h.debug("Segment max duration exceeded, creating new segment with time %v", l.Timestamp)
		nextSegment, err := h.createSegment(l.Domain, l.Timestamp)
		if err != nil {
			return nil, err
		}
		h.DomainSegments[l.Domain] = append(h.DomainSegments[l.Domain], nextSegment)
		return nextSegment, nil
	}

	// TODO: check against segment size limit, create if necessary

	return segment, nil
}

func (h *Hive) segmentsInRange(domain string, start, end time.Time) []*Segment {
	inRange := []*Segment{}
	for _, s := range h.DomainSegments[domain] {
		if s.Timestamp.After(start) && s.Timestamp.Before(end) {
			inRange = append(inRange, s)
		}
	}
	return inRange
}

func (h *Hive) segmentsAfter(domain string, start time.Time) []*Segment {
	inRange := []*Segment{}
	for _, s := range h.DomainSegments[domain] {
		if s.Timestamp.After(start) {
			inRange = append(inRange, s)
		}
	}
	return inRange
}

func (h *Hive) eachSegment(fn func(*Segment) error) error {
	var err error

	for _, segments := range h.DomainSegments {
		for _, segment := range segments {
			err = fn(segment)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (h *Hive) domainAllowed(domain string) bool {
	for _, d := range h.Domains {
		if d == domain {
			return true
		}
	}

	return false
}

// debug prints a log if Debug flag is set
func (h *Hive) debug(line string, v ...interface{}) {
	if h.Debug {
		fmt.Printf("[DEBUG] "+line, v...)
	}
}

func timestampFromSegmentFileName(name string) (time.Time, error) {
	timestampStr := strings.Replace(name, segmentFilePrefix, "", 1)
	timestamp, err := timeFromString(timestampStr)
	if err != nil {
		return time.Time{}, err
	}
	return timestamp, nil
}
