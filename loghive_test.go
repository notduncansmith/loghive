package loghive

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

type logstub struct {
	domain string
	line   string
}

func hive(t *testing.T, path string, domains []string) *Hive {
	os.RemoveAll(path)
	defer os.RemoveAll(path)
	config := DefaultConfig
	config.WritableDomains = domains
	h, err := NewHive(path, config)
	if err != nil {
		t.Errorf("Should be able to create hive: %v", err)
	}
	return h
}

func stubLogs(t *testing.T, h *Hive, stubs []logstub) {
	wg := sync.WaitGroup{}

	for _, stub := range stubs {
		wg.Add(1)
		flushed, err := h.Enqueue(stub.domain, []byte(stub.line))
		if err != nil {
			t.Errorf("Should be able to enqueue message: %v", err)
		}
		go func() {
			err := <-flushed
			if err != nil {
				t.Errorf("Should be able to write message %v", err)
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func checkResults(t *testing.T, h *Hive, q *Query, expected []logstub) {
	err := h.Query(q)
	if err != nil {
		t.Errorf("Should be able to execute query %v %v", q, err)
	}

	resultCount := 0

	for log := range q.Results {
		if resultCount == len(expected) {
			t.Errorf("Expected to get %v result(s), got %v", len(expected), resultCount+1)
			t.FailNow()
		}
		fmt.Printf("Result %v: %v\n", resultCount, log)
		expectedLine := string(expected[resultCount].line)
		expectedDomain := expected[resultCount].domain

		if string(log.Line) != expectedLine {
			t.Errorf("Expected line '%v', got '%v'", expectedLine, string(log.Line))
		}
		if log.Domain != expectedDomain {
			t.Errorf("Expected domain '%v', got '%v'", expectedDomain, string(log.Line))
		}
		resultCount++
	}

	if resultCount != len(expected) {
		t.Errorf("Expected to get %v result(s), got %v", len(expected), resultCount)
	}
}

func TestCreate(t *testing.T) {
	hive(t, "./fixtures/create", []string{"test"})
	os.Mkdir("./fixtures/unwritable", 0400)
	defer os.Remove("./fixtures/unwritable")
	h, err := NewHive("./fixtures/unwritable", DefaultConfig)
	if h != nil || err == nil {
		t.Error("Should not be able to create hive at unwritable dir")
	}
}

func TestRoundtripIterate(t *testing.T) {
	h := hive(t, "./fixtures/roundtrip_iterate", []string{"test"})
	stubLogs(t, h, []logstub{
		logstub{"test", "foo"},
	})

	chans := h.sm.Iterate([]string{"test"}, timestampNow().Add(time.Duration(-5)*time.Second), timestampNow(), 1, 1)
	read := <-(chans[0])
	if len(read) != 1 {
		t.Errorf("Expected to read 1 written msg, found %v", read)
		t.FailNow()
	}

	if string(read[0].Line) != "foo" {
		t.Errorf("Expected to find stored msg, found %v", read[0])
	}
}

func TestRoundtripQuery(t *testing.T) {
	h := hive(t, "./fixtures/roundtrip_query", []string{"test"})
	stubLogs(t, h, []logstub{
		logstub{"test", "foo"},
	})

	oneMinuteAgo := timestampNow().Add(time.Duration(-1) * time.Minute)
	q := NewQuery([]string{"test"}, oneMinuteAgo, timestampNow(), FilterMatchAll())

	checkResults(t, h, q, []logstub{
		logstub{"test", "foo"},
	})
}

func TestEnqueueValidation(t *testing.T) {
	h := hive(t, "./fixtures/roundtrip_query", []string{"test"})

	if _, err := h.Enqueue("nonexistentDomain", []byte("foo")); err == nil {
		t.Error("Should not be able to log to non-existent domain")
	}

	if _, err := h.Enqueue("test", nil); err == nil {
		t.Error("Should not be able to log non-existent line")
	}

	if _, err := h.Enqueue("test", make([]byte, 9*1024)); err == nil {
		t.Errorf("Should not be able to log line of %v bytes", 9*1024)
	}
}

func TestQueryValidation(t *testing.T) {
	h := hive(t, "./fixtures/roundtrip_query", []string{"test"})
	now := timestampNow()
	oneMinuteFromNow := now.Add(time.Duration(1) * time.Minute)
	twoMinutesFromNow := now.Add(time.Duration(2) * time.Minute)
	err := h.Query(NewQuery([]string{"test"}, oneMinuteFromNow, twoMinutesFromNow, FilterMatchAll()))
	if err == nil {
		t.Error("Should not be able to query the future")
	}

	oneMinuteAgo := now.Add(time.Duration(-1) * time.Minute)
	twoMinutesAgo := now.Add(time.Duration(-2) * time.Minute)
	err = h.Query(NewQuery([]string{"test"}, oneMinuteAgo, twoMinutesAgo, FilterMatchAll()))
	if err == nil {
		t.Error("Should not be able to query start after end")
	}

	err = h.Query(NewQuery([]string{"nonexistentDomain"}, twoMinutesAgo, oneMinuteAgo, FilterMatchAll()))
	if err == nil {
		t.Error("Should not be able to query nonexistent domain")
	}
}

func TestQueryFilters(t *testing.T) {
	h := hive(t, "./fixtures/roundtrip_query", []string{"test"})
	stubLogs(t, h, []logstub{
		logstub{"test", "foo"},
	})
	now := timestampNow()
	oneMinuteAgo := now.Add(time.Duration(-1) * time.Minute)

	queries := []*Query{
		NewQuery([]string{"test"}, oneMinuteAgo, now, FilterMatchAll()),
		NewQuery([]string{"test"}, oneMinuteAgo, now, FilterExactString("foo")),
		NewQuery([]string{"test"}, oneMinuteAgo, now, FilterContainsString("f")),
	}

	for _, q := range queries {
		checkResults(t, h, q, []logstub{
			logstub{"test", "foo"},
		})
	}

	negativeQueries := []*Query{
		NewQuery([]string{"test"}, oneMinuteAgo, now, FilterExactString("hello")),
		NewQuery([]string{"test"}, oneMinuteAgo, now, FilterContainsString("h")),
	}

	for _, q := range negativeQueries {
		checkResults(t, h, q, []logstub{})
	}
}
