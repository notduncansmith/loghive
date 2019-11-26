package loghive

import (
	"time"
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
	return &Query{domains, start, end, filter, make(chan *Log, 1024)}
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

func encodeKey(t time.Time) []byte {
	return timeToBytes(t)
}

func decodeKey(bz []byte) time.Time {
	return timeFromBytes(bz)
}
