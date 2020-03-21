# loghive

[![GoDoc](https://godoc.org/github.com/notduncansmith/loghive?status.svg)](https://godoc.org/github.com/notduncansmith/loghive) [![Build Status](https://travis-ci.com/notduncansmith/loghive.svg?branch=master)](https://travis-ci.com/notduncansmith/loghive) [![codecov](https://codecov.io/gh/notduncansmith/loghive/branch/master/graph/badge.svg)](https://codecov.io/gh/notduncansmith/loghive)

Loghive is an engine for storing and retrieving logs, built on SQLite.

## Why

Because storing and querying logs should be fast and easy, and it currently isn't. I've yet to find an open-source log manager that isn't either hideously overwrought or a freemium commercial offering. All I want is to put logs in, and get them back out fast, and be able to easily specify only the logs I need.

## Concepts

A Domain is a label that represents a single log history.

A Segment is a SQLite database that stores a contiguous chunk of a Domain's history starting from a point in time. Logs in a Segment are keyed by timestamp.

A Log is a struct with 3 fields:

```go
type Log struct {
	Domain    string    `json:"domain"`
	Timestamp time.Time `json:"timestamp"`
	Line      []byte    `json:"line"`
}
```

Logs can be queried by domain, timestamp, and line contents. Queries that span multiple domains will have their results serialized and delivered in timestamp order.

## Usage

```go
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
```

## License

Released under [The MIT License](https://opensource.org/licenses/MIT) (see `LICENSE.txt`).

Copyright 2019 Duncan Smith
