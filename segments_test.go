package loghive

import (
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func withSM(t *testing.T, path string, f func([]Segment, *SegmentManager)) {
	sm := NewSegmentManager(path)
	segments, err := sm.ScanDir()

	expectSuccess(t, "initialize SegmentManager", err)

	f(segments, sm)
	sm.Close()
}

func withTmp(t *testing.T, path string, f func([]Segment, *SegmentManager)) {
	os.RemoveAll(path)
	defer os.RemoveAll(path)
	withSM(t, path, f)
}

func TestSegmentManagerInit(t *testing.T) {
	withTmp(t, "./fixtures/sm_init", func(segments []Segment, sm *SegmentManager) {
		if len(segments) > 0 {
			t.Errorf("Expected to find no segments, found %v", segments)
		}
	})
}

func TestSegmentManagerCreate(t *testing.T) {
	withTmp(t, "./fixtures/sm_create", func(_ []Segment, sm *SegmentManager) {
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
		if len(segments) != 3 {
			t.Errorf("Expected 2 segments, got %v", segments)
		}
		s0 := segments[0]
		if s0.Path != "fixtures/sm_scan/.data/segment-2013-02-05T00:00:00Z" {
			t.Errorf("Incorrect segment path %v", s0.Path)
		}

		s1 := segments[1]
		if s1.Path != "fixtures/sm_scan/.data/segment-2019-12-11T06:47:11.072339Z" {
			t.Errorf("Incorrect segment path %v", s1.Path)
		}

		if len(sm.SegmentMap["test"]) != 2 {
			t.Errorf("Expected to find 2 test segments, found %v", sm.SegmentMap)
		}

		if len(sm.SegmentMap["otherdomain"]) != 1 {
			t.Errorf("Expected to find 1 otherdomain segments, found %v", sm.SegmentMap)
		}
	})

	os.RemoveAll("./fixtures/sm_scan_malformed")
	defer os.RemoveAll("./fixtures/sm_scan_malformed")

	os.MkdirAll("./fixtures/sm_scan_malformed/.data/segment-asdf", 0700)
	sm := NewSegmentManager("./fixtures/sm_scan_malformed")
	segments, err := sm.ScanDir()
	if err == nil || len(segments) > 0 {
		t.Errorf("Expected to find 0 segments, got %v %v", err, segments)
	}

	os.RemoveAll("./fixtures/sm_scan_malformed/.data/segment-asdf")
	os.Create("./fixtures/sm_scan_malformed/.data/segment-asdf")
	sm = NewSegmentManager("./fixtures/sm_scan_malformed")
	segments, err = sm.ScanDir()
	if err == nil || len(segments) > 0 {
		t.Errorf("Expected to find 0 segments, got %v %v", err, segments)
	}
}

func TestSegmentManagerWrite(t *testing.T) {
	withTmp(t, "./fixtures/sm_write", func(_ []Segment, sm *SegmentManager) {
		sm.CreateSegment("test1", timestamp())
		log1 := NewLog("test1", []byte("test"))
		time.Sleep(time.Second)
		sm.CreateSegment("test2", timestamp())
		log2 := NewLog("test2", []byte("test"))
		time.Sleep(time.Second)

		err := sm.Write([]*Log{log1, log2})

		if err != nil {
			t.Errorf("Expected to write logs, got errors %v", err)
		}

		backfilledLog := NewLog("test1", []byte("test"))
		backfilledLog.Timestamp = backfilledLog.Timestamp.Add(-1 * time.Hour)
		err = sm.Write([]*Log{backfilledLog})
		if err == nil {
			t.Error("Expected to get error when trying to write log prior to segment start")
		}
	})
}

func TestSegmentManagerRoundtrip(t *testing.T) {
	withTmp(t, "./fixtures/sm_roundtrip", func(_ []Segment, sm *SegmentManager) {
		epoch := timestamp()
		e1 := epoch.Add(time.Duration(-2) * time.Hour)
		e2 := epoch.Add(time.Duration(-1) * time.Hour)
		test1, err := sm.CreateSegment("test1", e1)
		expectSuccess(t, "create segment test1", err)
		test2, err := sm.CreateSegment("test2", e2)
		expectSuccess(t, "create segment test2", err)

		logrus.Println("Created segments", test1, test2)

		log1 := Log{"test1", e1.Add(time.Minute), []byte("hello")}
		log2 := Log{"test2", e2.Add(time.Minute), []byte("hello")}

		err = sm.Write([]*Log{&log1, &log2})

		if err != nil {
			t.Errorf("Expected to write logs, got error %v", err)
		}

		resultChans := sm.Iterate([]string{"test1", "test2"}, e1.Add(time.Duration(-1)*time.Hour), timestamp(), 1, 1)
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

func TestSegmentSizedOut(t *testing.T) {
	withTmp(t, "./fixtures/sm_sized_out", func(_ []Segment, sm *SegmentManager) {
		s, err := sm.CreateSegment("test", timestamp())
		expectSuccess(t, "create segment", err)
		l := NewLog("test", make([]byte, 128))
		expectSuccess(t, "write logs", sm.Write([]*Log{l}))

		maxBytes := int64(64)
		sizedOut, err := segmentSizedOut(s, maxBytes)
		expectSuccess(t, "check segment size", err)

		if !sizedOut {
			t.Errorf("Expected segment %v to be sized out (%v bytes)", s, maxBytes)
		}

		maxBytes = int64(1024)
		sizedOut, err = segmentSizedOut(s, maxBytes)
		expectSuccess(t, "checking segment size", err)

		if sizedOut {
			t.Errorf("Expected segment %v not to be sized out (%v bytes)", s, maxBytes)
		}
	})
}

func TestSegmentAgedOut(t *testing.T) {
	withTmp(t, "./fixtures/sm_aged_out", func(_ []Segment, sm *SegmentManager) {
		t1 := timestamp()
		t2 := t1.Add(time.Hour)
		t3 := t2.Add(time.Minute)

		s, err := sm.CreateSegment("test", t1)
		expectSuccess(t, "create segment", err)

		if !segmentAgedOut(s, t3, time.Hour) {
			t.Errorf("Expected segment %v to age out", s)
		}

		if segmentAgedOut(s, t2, 61*time.Minute) {
			t.Errorf("Expected segment %v not to age out", s)
		}
	})
}
