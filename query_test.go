package loghive

import (
	"testing"
	"time"
)

func TestQueryValidation(t *testing.T) {
	withHive(t, "./fixtures/roundtrip_query", []string{"test"}, func(h *Hive) {
		now := timestamp()
		oneMinuteFromNow := now.Add(time.Duration(1) * time.Minute)
		twoMinutesFromNow := now.Add(time.Duration(2) * time.Minute)
		err := h.Query(NewQuery([]string{"test"}, oneMinuteFromNow, twoMinutesFromNow, FilterMatchAll()))
		if err == nil {
			t.Error("Should not be able to query the future")
		}

		oneMinuteAgo := now.Add(time.Duration(-1) * time.Minute)
		twoMinutesAgo := now.Add(time.Duration(-2) * time.Minute)
		err = h.Query(NewQuery([]string{"test"}, oneMinuteAgo, twoMinutesAgo, FilterMatchAll()))
		if err == nil {
			t.Error("Should not be able to query start after end")
		}

		err = h.Query(NewQuery([]string{"nonexistentDomain"}, twoMinutesAgo, oneMinuteAgo, FilterMatchAll()))
		if err == nil {
			t.Error("Should not be able to query nonexistent domain")
		}
	})
}

func TestQueryFilters(t *testing.T) {
	withHive(t, "./fixtures/roundtrip_query", []string{"test"}, func(h *Hive) {
		stubLogs(t, h, []logstub{
			logstub{"test", "foo"},
		})
		now := timestamp()
		oneMinuteAgo := now.Add(time.Duration(-1) * time.Minute)

		queries := []*Query{
			NewQuery([]string{"test"}, oneMinuteAgo, now, FilterMatchAll()),
			NewQuery([]string{"test"}, oneMinuteAgo, now, FilterExactString("foo")),
			NewQuery([]string{"test"}, oneMinuteAgo, now, FilterContainsString("f")),
		}

		for _, q := range queries {
			checkResults(t, h, q, []logstub{
				logstub{"test", "foo"},
			})
		}

		negativeQueries := []*Query{
			NewQuery([]string{"test"}, oneMinuteAgo, now, FilterExactString("hello")),
			NewQuery([]string{"test"}, oneMinuteAgo, now, FilterContainsString("h")),
		}

		for _, q := range negativeQueries {
			checkResults(t, h, q, []logstub{})
		}
	})
}
