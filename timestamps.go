package loghive

import (
	"time"
)

func timestampNow() time.Time {
	return time.Now().UTC()
}

func timeFromBytes(bz []byte) time.Time {
	t := time.Time{}
	t.UnmarshalText(bz)
	return t
}

func timeToBytes(t time.Time) []byte {
	bz, _ := t.MarshalText()
	return bz
}
