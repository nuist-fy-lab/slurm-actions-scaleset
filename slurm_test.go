package main

import "testing"

func TestParseActiveJobs(t *testing.T) {
	got := parseActiveJobs("123|ghss-c2ln1-abcd1234\n124|other\n125|ghss-c2ln1-ffff0000\n")
	if len(got) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(got))
	}
	if got["c2ln1-abcd1234"].ID != 123 {
		t.Fatalf("unexpected job: %#v", got["c2ln1-abcd1234"])
	}
}
