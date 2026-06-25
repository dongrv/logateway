package observability

import (
	"testing"
	"time"
)

func TestRunProbeRecoversPanic(t *testing.T) {
	err := runProbe(func() error {
		panic("boom")
	}, time.Second)
	if err == nil {
		t.Fatal("expected panic error")
	}
}
