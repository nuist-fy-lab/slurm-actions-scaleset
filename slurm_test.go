package main

import "testing"

func TestParseActiveJobs(t *testing.T) {
	prefix := slurmJobPrefix("c2ln1-p1shr")
	got := parseActiveJobs("123|ghss-c2ln1-p1shr-c2ln1-abcd1234\n124|other\n125|ghss-c2ln1-scale-canary-c2ln1-wrong\n126|ghss-c2ln1-p1shr-c2ln1-ffff0000\n", prefix)
	if len(got) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(got))
	}
	if got["c2ln1-abcd1234"].ID != 123 {
		t.Fatalf("unexpected job: %#v", got["c2ln1-abcd1234"])
	}
}

func TestSlurmJobPrefixSanitizesScaleSetName(t *testing.T) {
	got := slurmJobPrefix("c2ln1/p1shr test")
	if got != "ghss-c2ln1-p1shr-test-" {
		t.Fatalf("unexpected prefix %q", got)
	}
}
