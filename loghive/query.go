package loghive

import (
	"sort"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger"
)

// Query is a set of parameters for querying logs
type Query struct {
	Filter  func(*Log) bool
	Domains []string
	Start   time.Time
	End     time.Time
	Results chan *Log
}

type si struct {
	*Segment
	*badger.Iterator
}

// NewQuery validates and builds a query from the given parameters
func NewQuery(filter func(*Log) bool, domains []string, start time.Time, end time.Time) (*Query, error) {
	return &Query{filter, domains, start, end, nil}, nil
}

func multiread(segments []*Segment, results chan *Log, start, end time.Time) error {
	wg := sync.WaitGroup{}
	sis := []*si{}
	mut := NewMutable("Segment Iterators")

	wg.Add(1)
	for _, segment := range segments {
		go func() {
			err := segment.DB.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()
				bz, _ := start.MarshalBinary()
				it.Seek(bz)
				mut.DoWithLock(func() {
					sis = append(sis, &si{segment, it})
				})
				wg.Wait()
				return nil
			})
			if err != nil {
				panic(err)
			}
		}()
	}

	for !allIteratorsInvalid(sis) && !allIteratorsOutOfRange(sis, end) {
		sortIterators(sis)
		sis[0].Item().Value(func(val []byte) error {
			timestamp := decodeKey(sis[0].Item().Key())
			results <- &Log{sis[0].Domain, timestamp, val}
			return nil
		})
		sis[0].Next()
	}

	return nil
}

func sortIterators(it []*si) {
	sort.Slice(it, func(a, b int) bool {
		if !it[a].Valid() {
			return false
		}
		if !it[b].Valid() {
			return true
		}
		ta := decodeKey(it[a].Item().Key())
		tb := decodeKey(it[b].Item().Key())
		return ta.Before(tb)
	})
}

func allIteratorsInvalid(its []*si) bool {
	invalid := 0
	for _, it := range its {
		if !it.Valid() {
			invalid++
		}
	}
	return invalid == len(its)
}

func allIteratorsOutOfRange(its []*si, end time.Time) bool {
	if end.Equal(time.Time{}) {
		return false
	}
	outOfRange := 0
	for _, it := range its {
		if end.Before(decodeKey(it.Item().Key())) {
			outOfRange++
		}
	}
	return outOfRange == len(its)
}

func decodeKey(bz []byte) time.Time {
	t := time.Time{}
	err := t.UnmarshalText(bz)
	if err != nil {
		panic("Error decoding key: " + string(bz) + ": " + err.Error())
	}
	return t
}

func encodeTime(t time.Time) []byte {
	bz, err := t.MarshalText()
	if err != nil {
		panic("Error encoding key: " + t.String() + ": " + err.Error())
	}
	return bz
}
