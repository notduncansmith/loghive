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

[The MIT License](https://opensource.org/licenses/MIT):

Copyright 2019 Duncan Smith

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.