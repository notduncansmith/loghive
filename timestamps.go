package loghive

import (
	"time"
)

func timestampNow() time.Time {
	return time.Now().UTC()
}

func timeToBytes(t time.Time) []byte {
	bz, _ := t.MarshalText()
	return bz
}
