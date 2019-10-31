package loghive

import (
	"fmt"
	"time"
)

func timeFromString(str string) (time.Time, error) {
	t := &time.Time{}
	if err := t.UnmarshalBinary([]byte(str)); err != nil {
		return time.Time{}, err
	}
	return *t, nil
}

func timeToString(t time.Time) string {
	bz, err := t.MarshalText()
	if err != nil {
		panic("Invalid time to string: " + fmt.Sprintf("%v", t.Unix()))
	}
	return string(bz)
}

func timestampNowSeconds() time.Time {
	return timestampNow().Round(0)
}

func timestampNow() time.Time {
	return time.Now().UTC()
}
