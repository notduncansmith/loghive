# loghive

Loghive is an engine for storing and retrieving logs, built on [Badger](https://github.com/dgraph-io/badger).

## Why

Because storing and querying logs should be fast and easy, and it currently isn't. I've yet to find an open-source log manager that isn't either hideously overwrought or a freemium commercial offering. All I want is to put logs in, and get them back out fast, and be able to easily specify only the logs I need.

## Concepts

A Domain is a label that represents a single log history.

A Segment is a Badger database that stores a contiguous chunk of a Domain's history starting from a point in time. Logs in a Segment are keyed by timestamp.

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
/* TODO */
```

## License

Released under [The MIT License](https://opensource.org/licenses/MIT) (see `LICENSE.txt`).

Copyright 2019 Duncan Smith