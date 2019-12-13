package loghive

import (
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
	fields := logrus.Fields{
		"method": ic.Method,
	}

	if l := ic.Log; l != nil {
		fields["domain"] = l.Domain
		fields["logTimestamp"] = l.Timestamp
		fields["logLine"] = string(l.Line)
	}

	if s := ic.Segment; s != nil {
		if fields["domain"] == "" {
			fields["domain"] = s.Domain
		}
		fields["segmentPath"] = s.Path
	}

	return logrus.WithFields(fields)
}
