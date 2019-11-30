package loghive

import (
	"strings"
	"time"

	"github.com/notduncansmith/march"
)

// Query is a set of parameters for querying logs
type Query struct {
	Domains []string
	Start   time.Time
	End     time.Time
	Filter  func(*Log) bool
	Results chan *Log
}

// NewQuery validates and builds a query from the given parameters
func NewQuery(domains []string, start time.Time, end time.Time, filter func(*Log) bool) *Query {
	return &Query{domains, start, end, filter, make(chan *Log)}
}

// ValidateQuery will return any error with a query's parameters
func (h *Hive) ValidateQuery(query *Query) error {
	for _, d := range query.Domains {
		if !h.domainValid(d) {
			return errInvalidQuery("invalid domain: " + d)
		}
	}

	if query.End.Before(query.Start) {
		return errInvalidQuery("end " + query.End.String() + " before start " + query.Start.String())
	}

	return nil
}

// Query will return the results of a query on a channel
func (h *Hive) Query(q *Query) error {
	err := h.ValidateQuery(q)
	if err != nil {
		return err
	}
	unfilteredResultChans := h.sm.Iterate(q.Domains, q.Start, q.End, 512, 8)
	unorderedResultChans := make([]chan march.Ordered, len(unfilteredResultChans))
	for idx, channel := range unfilteredResultChans {
		unorderedResultChans[idx] = make(chan march.Ordered)
		go func(i int, chunkChan chan []Log) {
			for chunk := range chunkChan {
				for _, log := range chunk {
					if q.Filter(&log) {
						unorderedResultChans[i] <- &log
					}
				}
			}
		}(idx, channel)
	}
	orderedResultChan := make(chan march.Ordered)
	go march.March(unorderedResultChans, orderedResultChan)
	go logify(orderedResultChan, q.Results)
	return nil
}

// FilterExactString takes a string and returns a filter matching lines with exactly that string
func FilterExactString(s string) func(l *Log) bool {
	return func(l *Log) bool {
		return string(l.Line) == s
	}
}

// FilterContainsString takes a string and returns a filter matching lines containing that string
func FilterContainsString(s string) func(l *Log) bool {
	return func(l *Log) bool {
		return strings.Contains(string(l.Line), s)
	}
}

// FilterMatchAll returns a filter matching all logs
func FilterMatchAll() func(l *Log) bool {
	return func(l *Log) bool {
		return true
	}
}

func logify(och chan march.Ordered, lch chan *Log) {
	for o := range och {
		log, ok := o.(*Log)
		if ok {
			lch <- log
		}
	}
	close(lch)
}
