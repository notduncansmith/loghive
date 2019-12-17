package loghive

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func expectSuccess(t *testing.T, task string, err error) {
	if err != nil {
		t.Errorf("Should be able to %v: %v", task, err)
	}
}

func expectError(t *testing.T, task string, err error) {
	if err == nil {
		t.Errorf("Should get error when trying to %v", task)
	}
}

func expectSegmentCount(t *testing.T, h *Hive, assertion, domain string, count int) {
	segmentCount := len(h.sm.SegmentMap[domain])
	if segmentCount != count {
		t.Errorf("%v: expected exactly %v segment(s), found %v", assertion, count, segmentCount)
	}
}

type logstub struct {
	domain string
	line   string
}

func withHive(t *testing.T, path string, domains []string, f func(*Hive)) *Hive {
	logrus.SetLevel(logrus.DebugLevel)
	os.RemoveAll(path)
	defer os.RemoveAll(path)
	config := DefaultConfig
	config.InternalLogLevel = logrus.DebugLevel
	config.WritableDomains = domains
	h, err := NewHive(path, config)
	if err != nil {
		t.Errorf("Should be able to create hive: %v", err)
	}
	for _, d := range domains {
		expectSegmentCount(t, h, "should start with 1 "+d+" segment", d, 1)
	}
	f(h)
	return h
}

func stubLogs(t *testing.T, h *Hive, stubs []logstub) {
	cbs := []chan error{}
	for _, stub := range stubs {
		flushed, err := h.Enqueue(stub.domain, []byte(stub.line))
		expectSuccess(t, "enqueue message", err)
		cbs = append(cbs, flushed)
	}

	for _, flushed := range cbs {
		err := <-flushed
		expectSuccess(t, "write message", err)
	}
}

func checkLogs(t *testing.T, h *Hive, q *Query, expected []logstub) {
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
		if err == nil {
			t.Error("Should not be able to create hive at unwritable dir")
		}
	})
}

func TestSegmentDeletion(t *testing.T) {
	withHive(t, "./fixtures/deletion", []string{"test"}, func(h *Hive) {
		h.config.SegmentMaxDuration = time.Second
		time.Sleep(time.Second)
		expectSegmentCount(t, h, "deleted segments", "test", 1)
		h.Enqueue("test", []byte("hello"))
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

		checkLogs(t, h, q, []logstub{
			logstub{"test", "foo"},
			logstub{"test", "bar"},
			logstub{"test", "baz"},
		})
	})
}

func TestRoundtripQueryMulti(t *testing.T) {
	oneMinuteAgo := timestamp().Add(time.Duration(-1) * time.Minute)

	withHive(t, "./fixtures/roundtrip_query_multi", []string{"test1", "test2"}, func(h *Hive) {
		h.config.SegmentMaxDuration = time.Second

		stubLogs(t, h, []logstub{
			logstub{"test1", "foo"},
			logstub{"test2", "foo"},
		})

		q1 := NewQuery([]string{"test1"}, oneMinuteAgo, timestamp(), FilterMatchAll())
		checkLogs(t, h, q1, []logstub{
			logstub{"test1", "foo"},
		})

		q2 := NewQuery([]string{"test1", "test2"}, oneMinuteAgo, timestamp(), FilterMatchAll())
		checkLogs(t, h, q2, []logstub{
			logstub{"test1", "foo"},
			logstub{"test2", "foo"},
		})
	})

	withHive(t, "./fixtures/roundtrip_query_multi", []string{"test1", "test2"}, func(h *Hive) {
		h.config.SegmentMaxDuration = time.Second

		stubLogs(t, h, []logstub{
			logstub{"test1", "foo"},
			logstub{"test2", "foo"},
		})

		time.Sleep(time.Second)

		stubLogs(t, h, []logstub{
			logstub{"test1", "bar"},
			logstub{"test2", "bar"},
		})

		time.Sleep(time.Second)

		q1 := NewQuery([]string{"test1"}, oneMinuteAgo, timestamp(), FilterMatchAll())
		checkLogs(t, h, q1, []logstub{
			logstub{"test1", "foo"},
			logstub{"test1", "bar"},
		})

		q2 := NewQuery([]string{"test1", "test2"}, oneMinuteAgo, timestamp(), FilterMatchAll())
		checkLogs(t, h, q2, []logstub{
			logstub{"test1", "foo"},
			logstub{"test2", "foo"},
			logstub{"test1", "bar"},
			logstub{"test2", "bar"},
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
		flushed, err := h.Enqueue("test", []byte(SyntheticFailureFlush))
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

func TestSizeSegmentCreationOnFlushSizeOut(t *testing.T) {
	withHive(t, "./fixtures/create_on_flush", []string{"test"}, func(h *Hive) {
		// note: config cannot be modified by code outside this package
		h.config.SegmentMaxBytes = 256
		stubLogs(t, h, []logstub{
			logstub{"test", "aaaaaaaa"},
		})
		expectSegmentCount(t, h, "should size out to 2 segments", "test", 2)
	})
}

func TestSegmentCreationOnFlushAgeOut(t *testing.T) {
	withHive(t, "./fixtures/create_on_flush", []string{"test"}, func(h *Hive) {
		h.config.SegmentMaxDuration = time.Second
		time.Sleep(time.Second)
		expectSegmentCount(t, h, "should not age out until triggered by flush", "test", 1)
		stubLogs(t, h, []logstub{
			logstub{"test", "foo"},
		})
		expectSegmentCount(t, h, "should age out to 2 segments after flush", "test", 2)
	})
}

func TestSegmentCreationOnFlushSizeOutLogFailure(t *testing.T) {
	withHive(t, "./fixtures/err_during_create_on_flush", []string{"test"}, func(h *Hive) {
		h.config.SegmentMaxBytes = 512
		flushed, _ := h.Enqueue("test", []byte(SyntheticFailureFlush))
		err := <-flushed
		expectError(t, "write doomed log", err)
		expectSegmentCount(t, h, "should still be 1 segment", "test", 1)
	})
}

func TestSegmentCreationOnFlushAgeOutLogFailure(t *testing.T) {
	withHive(t, "./fixtures/err_during_create_on_flush", []string{"test"}, func(h *Hive) {
		h.config.SegmentMaxDuration = time.Second
		time.Sleep(time.Second)
		flushed, _ := h.Enqueue("test", []byte(SyntheticFailureFlush))
		err := <-flushed
		expectError(t, "write doomed log", err)
		expectSegmentCount(t, h, "should still age out to 2 segments after failed flush", "test", 2)
	})
}

func TestSegmentWriteFailure(t *testing.T) {
	withHive(t, "./fixtures/err_during_write", []string{"test"}, func(h *Hive) {
		flushed, _ := h.Enqueue("test", []byte(SyntheticFailureWrite))
		err := <-flushed
		expectError(t, "write doomed log", err)
	})
}

func TestNeededSegmentsCreationError(t *testing.T) {
	// Note: this test demonstrates that BadgerDB does NOT return an
	// error when I believe it should (writing to missing file). Open issue: https://github.com/dgraph-io/badger/issues/1159
	withHive(t, "./fixtures/err_during_create_on_flush", []string{"test"}, func(h *Hive) {
		h.config.SegmentMaxDuration = time.Second
		sh("rm", "-rf", h.sm.segmentPath(h.sm.SegmentMap["test"][0].Path))
		time.Sleep(time.Second)
		flushed, _ := h.Enqueue("test", []byte("foo"))
		err := <-flushed
		expectSuccess(t, "write log to unwritable directory", err)
	})
}

func sh(name string, args ...string) string {
	out, err := exec.Command(name, args...).Output()
	fmt.Println(string(out), err)
	return string(out)
}
