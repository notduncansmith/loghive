package loghive

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/notduncansmith/march"
)

func TestRoundtrip(t *testing.T) {
	h, err := NewHive("./fixtures/roundtrip")
	if err != nil {
		t.Errorf("Should be able to create hive: %v", err)
	}
	fmt.Println("Hive created")

	err = h.UpdateConfig(func(c Config) Config {
		c.m["WritableDomains"] = append(c.m["WritableDomains"].([]string), "test")
		return c
	})

	fmt.Println("Hive configured")

	if err != nil {
		t.Errorf("Should be able to configure hive: %v", err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		h.Enqueue("test", []byte("foo"))
		fmt.Println("Msg enqueued")
		time.Sleep(time.Duration(1) * time.Second)
		fmt.Println("Building query")
		q := Query{
			Domains: []string{"test"},
			Start:   time.Now().Add(time.Duration(-2) * time.Second),
			End:     time.Now(),
			Filter: func(l *Log) bool {
				return string(l.Line) == "foo"
			},
			Results: make(chan march.Ordered),
		}
		fmt.Println("Querying")
		h.Query(q)
		fmt.Println("Reading results")
		for ordered := range q.Results {
			fmt.Printf("Result 1: %v\n", ordered)
			log, ok := ordered.(*Log)
			if !ok || string(log.Line) != "foo" {
				t.Errorf("Expected line 'foo', got '%v'", string(log.Line))
			}
		}
		wg.Done()
	}()
	wg.Wait()
}
