package domain

import "testing"

func TestParsePageQuery(t *testing.T) {
	p, err := ParsePageQuery("", "")
	if len(err) != 0 || p.Limit != DefaultListLimit {
		t.Fatalf("%v %v", p, err)
	}
	_, err = ParsePageQuery("101", "0")
	if len(err) == 0 {
		t.Fatal("expected too_large")
	}
}

func TestParseStatusFilter(t *testing.T) {
	_, err := ParseStatusFilter("bogus")
	if len(err) != 1 {
		t.Fatalf("%v", err)
	}
	s, err := ParseStatusFilter("queued")
	if len(err) != 0 || s != StatusQueued {
		t.Fatalf("%v %s", err, s)
	}
}
