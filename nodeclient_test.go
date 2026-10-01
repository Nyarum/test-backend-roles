package inferno

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
)

func TestDispatchReusesNodeConnections(t *testing.T) {
	pool := testPool(t)

	const (
		accountID = 3001
		total     = 300
	)

	for _, concurrency := range []int{1, 8} {
		t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
			seedAccount(t, pool, accountID, 1_000_000)

			node := newFakeNode(t, 5, 0)
			d := New(pool, Config{NodeURL: node.URL, PricePerRequest: 1})
			defer d.Close()

			jobs := make(chan int)
			var wg sync.WaitGroup
			for w := 0; w < concurrency; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range jobs {
						if err := d.Dispatch(context.Background(), accountID, "conn", io.Discard); err != nil {
							t.Errorf("request %d: %v", i, err)
						}
					}
				}()
			}
			for i := 0; i < total; i++ {
				jobs <- i
			}
			close(jobs)
			wg.Wait()

			limit := int64(concurrency + 2)
			if got := node.newConns.Load(); got > limit {
				t.Errorf("node accepted %d new connections for %d requests at concurrency %d, want at most %d",
					got, total, concurrency, limit)
			}
		})
	}
}
