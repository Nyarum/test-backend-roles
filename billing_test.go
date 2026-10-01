package inferno

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestChargeConcurrentRequests(t *testing.T) {
	pool := testPool(t)
	d := New(pool, Config{PricePerRequest: 10})
	defer d.Close()

	const (
		accountID = 1001
		callers   = 50
		cost      = 10
	)
	seedAccount(t, pool, accountID, 100)

	var charged, rejected atomic.Int64
	start := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := d.Charge(context.Background(), accountID, cost)
			switch {
			case err == nil:
				charged.Add(1)
			case errors.Is(err, ErrInsufficientCredits):
				rejected.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := charged.Load(); got != 10 {
		t.Errorf("successful charges = %d, want 10", got)
	}
	if got := rejected.Load(); got != 40 {
		t.Errorf("rejected charges = %d, want 40", got)
	}
	if got := accountCredits(t, pool, accountID); got != 0 {
		t.Errorf("final balance = %d, want 0", got)
	}
}
