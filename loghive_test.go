package loghive

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestCreate(t *testing.T) {
	defer os.RemoveAll("./fixtures/create")
	_, err := NewHive("./fixtures/create", DefaultConfig)
	if err != nil {
		t.Errorf("Should be able to create hive: %v", err)
	}
	fmt.Println("Hive created")
}

func TestRoundtripIterate(t *testing.T) {
	path := "./fixtures/roundtrip_iterate"
	os.RemoveAll(path)
	defer os.RemoveAll(path)
	config := DefaultConfig
	config.WritableDomains = []string{"test"}
	config.FlushAfterItems = 1
	config.FlushAfterDuration = time.Duration(0)
	h, err := NewHive(path, config)
	if err != nil {
		t.Errorf("Should be able to create hive: %v", err)
	}

	flushed, err := h.Enqueue("test", []byte("foo"))
	if err != nil {
		t.Errorf("Should be able to enqueue message: %v", err)
	}
	err = <-flushed
	if err != nil {
		t.Errorf("Should be able to write message %v", err)
	}

	chans := h.sm.Iterate([]string{"test"}, time.Now().Add(time.Duration(-5)*time.Second), time.Now(), 1, 1)
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
	path := "./fixtures/roundtrip_query"
	os.RemoveAll(path)
	defer os.RemoveAll(path)
	config := DefaultConfig
	config.WritableDomains = []string{"test"}
	h, err := NewHive(path, config)
	if err != nil {
		t.Errorf("Should be able to create hive: %v", err)
	}
	flushed, err := h.Enqueue("test", []byte("foo"))
	if err != nil {
		t.Errorf("Should be able to enqueue message: %v", err)
	}
	<-flushed
	oneMinuteAgo := time.Now().Add(time.Duration(-1) * time.Minute)
	q := NewQuery([]string{"test"}, oneMinuteAgo, time.Now(), FilterMatchAll())
	go func() {
		err = h.Query(q)
		if err != nil {
			t.Errorf("Should be able to execute query %v %v", q, err)
		}
	}()

	gotResults := false
	for log := range q.Results {
		gotResults = true
		fmt.Printf("Result 1: %v\n", log)
		if string(log.Line) != "foo" {
			t.Errorf("Expected line 'foo', got '%v'", string(log.Line))
		}
	}
	if !gotResults {
		t.Error("Expected to get results")
	}
}
