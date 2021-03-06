package loghive

import (
	"errors"
	"fmt"
	"time"
)

// LogWriteFailure provides information about why one or more logs were not written
type LogWriteFailure struct {
	Err   error `json:"err"`
	Log   Log   `json:"log"`
	Count int   `json:"count"`
}

func coalesceLogWriteFailures(errs []LogWriteFailure) error {
	if len(errs) == 0 {
		return nil
	}
	msg := "Unable to write one or more logs: "
	for i, e := range errs {
		msg += fmt.Sprintf("\n[%v] %v", i, e)
	}
	return errors.New(msg)
}

func coalesceErrors(process string, errs []error) error {
	if len(errs) == 0 {
		return nil
	}

	if len(errs) == 1 {
		return errs[0]
	}

	msg := "Errors (" + process + "):"
	for _, e := range errs {
		msg += "\n" + e.Error()
	}
	return errors.New(msg)
}

func errUnreachable(path string, reason string) error {
	return fmt.Errorf("File or directory %v cannot be opened (%v)", path, reason)
}

func errMalformedSegment(path string, err error) error {
	return fmt.Errorf("Segment %v cannot be read (%v)", path, err)
}

func errInvalidLogDomain(domain string) error {
	return fmt.Errorf("Attempted to log to invalid domain: %v", domain)
}

func errUnableToBackfill(domain string, segmentTime, timestamp time.Time) error {
	return fmt.Errorf("Attempted to backfill log in domain %v (segment time %v) with timestamp %v", domain, string(timeToBytes(segmentTime)), string(timeToBytes(timestamp)))
}

func errLineMissing() error {
	return fmt.Errorf("Attempted to log empty line")
}

func errLineTooLarge(count, max int) error {
	return fmt.Errorf("Attempted to log line of %v bytes (max: %v)", count, max)
}

func errInvalidQuery(reason string) error {
	return fmt.Errorf("Invalid query (%v)", reason)
}
