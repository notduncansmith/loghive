package loghive

import (
	"errors"
	"strconv"
	"time"
)

func errUnableToLoadConfig(reason string) error {
	return errors.New("Unable to load config: " + reason)
}

func errUnreachable(path string, reason string) error {
	return errors.New("File or directory " + path + " cannot be opened (" + reason + ")")
}

func errQueueFull() error {
	return errors.New("Queue is full")
}

func errInvalidSegmentFilename(filename string) error {
	return errors.New("Invalid segment filename: " + filename)
}

func errUnableToOpenFile(filename string, reason string) error {
	return errors.New("Unable to open file: " + filename + " (" + reason + ")")
}

func errInvalidLogDomain(domain string) error {
	return errors.New("Attempted to log to invalid domain: " + domain)
}

func errUnableToBackfill(domain string, timestamp time.Time) error {
	return errors.New("Attempted to backfill log in domain " + domain + " with timestamp " + string(timeToBytes(timestamp)))
}

func errLineTooLarge(count, max int) error {
	return errors.New("Attempted to log line of " + strconv.Itoa(count) + " bytes (max: " + strconv.Itoa(max) + ")")
}

func errInvalidQuery(reason string) error {
	return errors.New("Invalid query (" + reason + ")")
}
