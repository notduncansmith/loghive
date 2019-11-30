package loghive

import (
	"github.com/notduncansmith/march"
	"time"
)

// Query is a set of parameters for querying logs
type Query struct {
	Domains []string
	Start   time.Time
	End     time.Time
	Filter  func(*Log) bool
	Results chan march.Ordered
}

// NewQuery validates and builds a query from the given parameters
func NewQuery(domains []string, start time.Time, end time.Time, filter func(*Log) bool) *Query {
	return &Query{domains, start, end, filter, make(chan march.Ordered, 1024)}
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
func (h *Hive) Query(q Query) error {
	err := h.ValidateQuery(&q)
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
	return nil
}
