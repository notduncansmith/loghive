package loghive

import (
	"time"

	"github.com/sirupsen/logrus"
)

// IC is short for Internal (Log) Context. This makes it easy to do structured logging when processing logs
type IC struct {
	Method  string
	Log     *Log
	Segment *Segment
}

func newIC(method string) IC {
	return IC{method, nil, nil}
}

func (ic IC) clone() IC {
	return ic
}

func (ic IC) withLog(l *Log) IC {
	c := ic.clone()
	c.Log = l
	return c
}

func (ic IC) withSegment(s *Segment) IC {
	c := ic.clone()
	c.Segment = s
	return c
}

// L returns a logrus logger with fields set from the log context
func (ic IC) L() *logrus.Entry {
	var domain string
	var logTimestamp time.Time
	var logLine string
	var segmentPath string

	if l := ic.Log; l != nil {
		domain = l.Domain
		logTimestamp = l.Timestamp
		logLine = string(l.Line)
	}

	if s := ic.Segment; s != nil {
		segmentPath = s.Path
	}

	return logrus.WithFields(logrus.Fields{
		"method":       ic.Method,
		"domain":       domain,
		"logTimestamp": logTimestamp,
		"logLine":      logLine,
		"segmentPath":  segmentPath,
	})
}
