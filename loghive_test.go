package loghive

import (
	"fmt"
	"testing"
	"time"
)

func TestCreate(t *testing.T) {
	_, err := NewHive("./fixtures/create", DefaultConfig)
	if err != nil {
		t.Errorf("Should be able to create hive: %v", err)
	}
	fmt.Println("Hive created")
}

func TestRoundtrip(t *testing.T) {
	config := DefaultConfig
	config.WritableDomains = append(config.WritableDomains, "test")
	config.FlushAfterItems = 1
	config.FlushAfterDuration = time.Duration(0)
	h, err := NewHive("./fixtures/roundtrip", config)
	if err != nil {
		t.Errorf("Should be able to create hive: %v", err)
	}
	fmt.Println("Hive created")

	h.Enqueue("test", []byte("foo"))
	fmt.Println("Msg enqueued, building query")
	twoSecondsAgo := time.Now().Add(time.Duration(-2) * time.Second)
	q := NewQuery([]string{"test"}, twoSecondsAgo, time.Now(), FilterExactString("foo"))
	fmt.Println("Querying")
	go h.Query(q)
	fmt.Println("Reading results")
	for log := range q.Results {
		fmt.Printf("Result 1: %v\n", log)
		if string(log.Line) != "foo" {
			t.Errorf("Expected line 'foo', got '%v'", string(log.Line))
		}
	}
}
