package loghive

import "time"

// Log is a single log line within a domain
type Log struct {
	Domain    string    `json:"domain"`
	Timestamp time.Time `json:"timestamp"`
	Line      []byte    `json:"line"`
}

// NewLog constructs a timestamped Log from a domain and line
func NewLog(domain string, line []byte) *Log {
	return &Log{domain, timestampNow(), line}
}

func (l *Log) String() string {
	return "Log{" + string(timeToBytes(l.Timestamp)) + ", " + l.Domain + ", <<" + string(l.Line) + ">>}"
}
