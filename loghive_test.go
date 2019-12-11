package loghive

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func expectSuccess(t *testing.T, task string, err error) {
	if err != nil {
		t.Errorf("Should be able to %v: %v", task, err)
	}
}

type logstub struct {
	domain string
	line   string
}

func withHive(t *testing.T, path string, domains []string, f func(*Hive)) *Hive {
	os.RemoveAll(path)
	defer os.RemoveAll(path)
	config := DefaultConfig
	config.InternalLogLevel = logrus.DebugLevel
	config.WritableDomains = domains
	h, err := NewHive(path, config)
	if err != nil {
		t.Errorf("Should be able to create hive: %v", err)
	}
	f(h)
	return h
}

func stubLogs(t *testing.T, h *Hive, stubs []logstub) {
	wg := sync.WaitGroup{}

	for _, stub := range stubs {
		wg.Add(1)
		flushed, err := h.Enqueue(stub.domain, []byte(stub.line))
		expectSuccess(t, "enqueue message", err)
		go func(f chan error) {
			err := <-f
			expectSuccess(t, "write message", err)
			wg.Done()
		}(flushed)
	}

	wg.Wait()
}

func checkResults(t *testing.T, h *Hive, q *Query, expected []logstub) {
	err := h.Query(q)
	expectSuccess(t, fmt.Sprintf("execute query %v", q), err)

	resultCount := 0

	for log := range q.Results {
		if resultCount == len(expected) {
			t.Errorf("Expected to get %v result(s), got %v", len(expected), resultCount+1)
			t.FailNow()
		}
		logrus.Printf("Result %v: %v\n", resultCount, log)
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
	withHive(t, "./fixtures/create", []string{"test"}, func(h *Hive) {
		os.Mkdir("./fixtures/unwritable", 0400)
		defer os.Remove("./fixtures/unwritable")
		h, err := NewHive("./fixtures/unwritable", DefaultConfig)
		if h != nil || err == nil {
			t.Error("Should not be able to create hive at unwritable dir")
		}
	})
}

func TestRoundtripIterate(t *testing.T) {
	withHive(t, "./fixtures/roundtrip_iterate", []string{"test"}, func(h *Hive) {
		stubLogs(t, h, []logstub{
			logstub{"test", "foo"},
		})

		chans := h.sm.Iterate([]string{"test"}, timestamp().Add(time.Duration(-5)*time.Second), timestamp(), 1, 1)
		read := <-(chans[0])
		if len(read) != 1 {
			t.Errorf("Expected to read 1 written msg, found %v", read)
			t.FailNow()
		}

		if string(read[0].Line) != "foo" {
			t.Errorf("Expected to find stored msg, found %v", read[0])
		}
	})
}

func TestRoundtripQuery(t *testing.T) {
	withHive(t, "./fixtures/roundtrip_query", []string{"test"}, func(h *Hive) {
		stubLogs(t, h, []logstub{
			logstub{"test", "foo"},
			logstub{"test", "bar"},
			logstub{"test", "baz"},
		})

		oneMinuteAgo := timestamp().Add(time.Duration(-1) * time.Minute)
		q := NewQuery([]string{"test"}, oneMinuteAgo, timestamp(), FilterMatchAll())

		checkResults(t, h, q, []logstub{
			logstub{"test", "foo"},
			logstub{"test", "bar"},
			logstub{"test", "baz"},
		})
	})
}

func TestEnqueueValidation(t *testing.T) {
	withHive(t, "./fixtures/enqueue_validation", []string{"test"}, func(h *Hive) {
		if _, err := h.Enqueue("nonexistentDomain", []byte("foo")); err == nil {
			t.Error("Should not be able to log to non-existent domain")
		}

		if _, err := h.Enqueue("test", nil); err == nil {
			t.Error("Should not be able to log non-existent line")
		}

		if _, err := h.Enqueue("test", make([]byte, 9*1024)); err == nil {
			t.Errorf("Should not be able to log line of %v bytes", 9*1024)
		}
	})
}

func TestErrWhileFlushing(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	withHive(t, "./fixtures/err_while_flushing", []string{"test"}, func(h *Hive) {
		flushed, err := h.Enqueue("test", []byte("shouldFail"))
		expectSuccess(t, "enqueue", err)

		for e := range flushed {
			if e != nil {
				err = e
			}
			logrus.Println(e)
		}

		if err == nil {
			t.Error("Flush should have failed")
		}
	})
}

func TestErrOpening(t *testing.T) {
	os.RemoveAll("./fixtures/untouchable_dir")
	os.MkdirAll("./fixtures/untouchable_dir/untouchable", 0700)
	defer os.RemoveAll("./fixtures/untouchable_dir")
	os.Chmod("./fixtures/untouchable_dir/untouchable", 0400)

	config := DefaultConfig
	config.InternalLogLevel = logrus.DebugLevel
	_, err := NewHive("./fixtures/untouchable_dir/untouchable", config)
	if err == nil {
		t.Error("Should fail to open in untouchable directory")
	}
}
