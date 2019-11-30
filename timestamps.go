package loghive

import (
	"time"
)

func timestampNowSeconds() time.Time {
	return timestampNow().Round(0)
}

func timestampNow() time.Time {
	return time.Now().UTC()
}

func timeFromBytes(bz []byte) time.Time {
	t := time.Time{}
	err := t.UnmarshalText(bz)
	if err != nil {
		panic("Error decoding time: " + string(bz) + ": " + err.Error())
	}
	return t
}

func timeToBytes(t time.Time) []byte {
	bz, err := t.MarshalText()
	if err != nil {
		panic("Error encoding time: " + t.String() + ": " + err.Error())
	}
	return bz
}
