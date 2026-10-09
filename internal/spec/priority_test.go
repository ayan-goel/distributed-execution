package spec

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestJobPriorityContractAndCanonicalIdentity(t *testing.T) {
	base := string(example(t))
	original, err := DecodeJob(strings.NewReader(base))
	if err != nil {
		t.Fatal(err)
	}
	oldBytes, oldHash, err := original.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(oldBytes, []byte(`"priority"`)) {
		t.Fatal("default priority changed existing serialized documents")
	}
	for priority := range 4 {
		t.Run(fmt.Sprint(priority), func(t *testing.T) {
			input := strings.Replace(base, "spec:\n", fmt.Sprintf("spec:\n  priority: %d\n", priority), 1)
			job, err := DecodeJob(strings.NewReader(input))
			if err != nil {
				t.Fatal("bounded integer priority rejected", err)
			}
			canonical, hash, err := job.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			if priority == 0 {
				if hash != oldHash || !bytes.Equal(canonical, oldBytes) {
					t.Fatal("explicit default changed existing canonical identity")
				}
			} else if hash == oldHash || !bytes.Contains(canonical, []byte(fmt.Sprintf(`"priority":%d`, priority))) {
				t.Fatal("priority was not bound to request identity")
			}
			roundTrip, err := DecodeJob(bytes.NewReader(canonical))
			if err != nil {
				t.Fatal(err)
			}
			_, again, err := roundTrip.Canonical()
			if err != nil || again != hash {
				t.Fatal("priority did not round trip", err)
			}
		})
	}
	for _, invalid := range []string{"-1", "4", "256", "0.5", `"3"`, "null", "true", "[1]", "{}"} {
		input := strings.Replace(base, "spec:\n", "spec:\n  priority: "+invalid+"\n", 1)
		if _, err := DecodeJob(strings.NewReader(input)); err == nil {
			t.Fatalf("invalid priority %s accepted", invalid)
		}
	}
	for _, invalid := range []int{-1, 4, 256} {
		original.Spec.Priority = invalid
		if _, _, err := original.Canonical(); err == nil {
			t.Fatalf("programmatic priority %d bypassed validation", invalid)
		}
	}
}
