package loghive

import (
	"sort"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger"
)

type si struct {
	*Segment
	*badger.Iterator
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

func decodeKey(key []byte) time.Time {
	t := time.Time{}
	err := t.UnmarshalText(key)
	if err != nil {
		panic("Error decoding key: " + string(key) + ": " + err.Error())
	}
	return t
}

func encodeTime(t time.Time) []byte {
	bz, err := t.MarshalText()
	if err != nil {
		panic("Error decoding key: " + string(bz) + ": " + err.Error())
	}
	return bz
}
