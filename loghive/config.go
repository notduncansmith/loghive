package loghive

import "time"

// Config describes the configuration that Loghive needs to function
type Config struct {
	Debug                    bool          `json:"debug"`
	QueueSize                int           `json:"queueSize"`
	StoragePath              string        `json:"storagePath"`
	Domains                  []string      `json:"domains"`
	MaxSegments              int           `json:"maxSegments"`
	SegmentMaxDuration       time.Duration `json:"segmentMaxDuration"`
	SegmentMaxBytes          int           `json:"segmentMaxBytes"`
	LineMaxBytes             int           `json:"lineMaxBytes"`
	QueryMaxQueueSize        int           `json:"tailMaxQueueSize"`
	QueryMaxExpressionLength int           `json:"queryMaxExpressionLength"`
	QueryBufferBytes         int           `json:"queryBufferBytes"`
}
