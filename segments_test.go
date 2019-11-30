package loghive

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func expectSuccess(t *testing.T, task string, err error) {
	if err != nil {
		t.Errorf("Should be able to %v: %v", task, err)
	}
}

func withSM(t *testing.T, path string, f func([]Segment, *SegmentManager)) {
	sm := NewSegmentManager(path)
	segments, err := sm.ScanDir()

	expectSuccess(t, "initialize SegmentManager", err)

	f(segments, sm)
	sm.Close()
}

func TestSegmentManagerInit(t *testing.T) {
	os.RemoveAll("./fixtures/sm_init")
	defer os.RemoveAll("./fixtures/sm_init")
	withSM(t, "./fixtures/sm_init", func(segments []Segment, sm *SegmentManager) {
		if len(segments) > 0 {
			t.Errorf("Expected to find no segments, found %v", segments)
		}
	})
}

func TestSegmentManagerCreate(t *testing.T) {
	os.RemoveAll("./fixtures/sm_create")
	defer os.RemoveAll("./fixtures/sm_create")
	withSM(t, "./fixtures/sm_create", func(_ []Segment, sm *SegmentManager) {
		epoch, err := time.Parse("2006-Jan-02", "2013-Feb-05")
		if err != nil {
			t.Errorf("unable to parse date %v", err)
		}
		s, err := sm.CreateSegment("test", epoch)
		expectSuccess(t, "create Segment", err)
		if s.Path != "fixtures/sm_create/.data/segment-2013-02-05T00:00:00Z" {
			t.Errorf("Incorrect segment path %v", s.Path)
		}
		if s.Domain != "test" {
			t.Errorf("Incorrect segment domain %v", s.Domain)
		}
		if !s.Timestamp.Equal(epoch) {
			t.Errorf("Incorrect segment timestamp %v", s.Timestamp)
		}
	})
}

func TestSegmentManagerScan(t *testing.T) {
	withSM(t, "./fixtures/sm_scan", func(segments []Segment, sm *SegmentManager) {
		if len(segments) != 1 {
			t.Errorf("Expected 1 segment, got %v", segments)
		}
		s := segments[0]
		if s.Path != "fixtures/sm_scan/.data/segment-2013-02-05T00:00:00Z" {
			t.Errorf("Incorrect segment path %v", s.Path)
		}
	})
}

func TestSegmentManagerWrite(t *testing.T) {
	os.RemoveAll("./fixtures/sm_write")
	defer os.RemoveAll("./fixtures/sm_write")
	withSM(t, "./fixtures/sm_write", func(_ []Segment, sm *SegmentManager) {
		sm.CreateSegment("test1", time.Now())
		log1 := NewLog("test1", []byte("test"))
		time.Sleep(time.Second)
		sm.CreateSegment("test2", time.Now())
		log2 := NewLog("test2", []byte("test"))
		time.Sleep(time.Second)

		errs := sm.Write([]Log{*log1, *log2})

		if len(errs) > 0 {
			t.Errorf("Expected to write logs, got errors %v", errs)
		}
	})
}

func TestSegmentManagerRoundtrip(t *testing.T) {
	os.RemoveAll("./fixtures/sm_roundtrip")
	defer os.RemoveAll("./fixtures/sm_roundtrip")
	withSM(t, "./fixtures/sm_roundtrip", func(_ []Segment, sm *SegmentManager) {
		epoch := time.Now()
		e1 := epoch.Add(time.Duration(-2) * time.Hour)
		e2 := epoch.Add(time.Duration(-1) * time.Hour)
		test1, err := sm.CreateSegment("test1", e1)
		expectSuccess(t, "create segment test1", err)
		test2, err := sm.CreateSegment("test2", e2)
		expectSuccess(t, "create segment test2", err)

		fmt.Println("Created segments", test1, test2)

		log1 := Log{"test1", e1.Add(time.Minute), []byte("hello")}
		log2 := Log{"test2", e2.Add(time.Minute), []byte("hello")}

		errs := sm.Write([]Log{log1, log2})

		if len(errs) > 0 {
			t.Errorf("Expected to write logs, got errors %v", errs)
		}

		resultChans := sm.Iterate([]string{"test1", "test2"}, e1.Add(time.Duration(-1)*time.Hour), time.Now(), 1, 1)
		result1, open1 := <-resultChans[0]
		result2, open2 := <-resultChans[1]
		if !open1 {
			t.Error("Expected result channel 1 to stay open")
		}
		if !open2 {
			t.Error("Expected result channel 2 to stay open")
		}
		if len(result1) == 1 {
			r := result1[0]
			if r.Domain != "test1" {
				t.Errorf("Incorrect log domain %v", r.Domain)
			}
			if string(r.Line) != "hello" {
				t.Errorf("Incorrect log line %v", r.Line)
			}
			if !r.Timestamp.Equal(log1.Timestamp) {
				t.Errorf("Incorrect log timestamp %v", r.Timestamp)
			}
		} else {
			t.Errorf("Expected 1 log per chunk, got %v", result1)
		}

		if len(result2) == 1 {
			r := result2[0]
			if r.Domain != "test2" {
				t.Errorf("Incorrect log domain %v", r.Domain)
			}
			if string(r.Line) != "hello" {
				t.Errorf("Incorrect log line %v", r.Line)
			}
			if !r.Timestamp.Equal(log2.Timestamp) {
				t.Errorf("Incorrect log timestamp %v", r.Timestamp)
			}
		} else {
			t.Errorf("Expected 1 log per chunk, got %v", result2)
		}
	})
}
