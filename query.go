package loghive

import (
	"strings"
	"time"

	"github.com/notduncansmith/march"
	"github.com/sirupsen/logrus"
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

	now := timestamp()
	if query.Start.After(now) {
		return errInvalidQuery("start " + query.Start.String() + " before now " + now.String())
	}

	return nil
}

// Query will return the results of a query on a channel
func (h *Hive) Query(q *Query) error {
	err := h.ValidateQuery(q)
	if err != nil {
		return err
	}
	logrus.Debugf("Iterating domains %v\n", q.Domains)
	unfilteredDomainResultChans := h.sm.Iterate(q.Domains, q.Start, q.End, 512, 8)
	unorderedDomainResultChans := make([]chan march.Ordered, len(unfilteredDomainResultChans))
	for idx, channel := range unfilteredDomainResultChans {
		unorderedDomainResultChans[idx] = make(chan march.Ordered)
		go func(i int, chunkChan chan []Log) {
			defer close(unorderedDomainResultChans[i])
			for chunk := range chunkChan {
				logrus.Debugf("Got chunk of size %v in domain %v: %v", len(chunk), q.Domains[i], chunk)
				for _, log := range chunk {
					copy := log // pointers to `log` variable will point to new iteration values
					logrus.Debugf("Filtering log: %v", log)
					if q.Filter(&copy) {
						logrus.Debugf("Log accepted: [%v] %v", i, log)
						unorderedDomainResultChans[i] <- &copy
					} else {
						logrus.Debugf("Log rejected: %v", log)
					}
				}
			}

			logrus.Debugf("Done with domain %v", q.Domains[i])
		}(idx, channel)
	}
	orderedCrossDomainResultChan := make(chan march.Ordered)
	go march.March(unorderedDomainResultChans, orderedCrossDomainResultChan)
	go logify(orderedCrossDomainResultChan, q.Results)
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
		logrus.Debugf("Got ordered log %v", log)
		if ok {
			lch <- log
		}
	}
	close(lch)
}
