package inferno

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDispatchStreamsOnlyOwnTokens(t *testing.T) {
	pool := testPool(t)

	const (
		requests = 100
		tokens   = 20
		firstID  = 2000
	)

	node := newFakeNode(t, tokens, 2*time.Millisecond)
	d := New(pool, Config{NodeURL: node.URL, PricePerRequest: 1})

	for i := 0; i < requests; i++ {
		seedAccount(t, pool, firstID+int64(i), 1000)
	}

	sinks := make([]bytes.Buffer, requests)
	errs := make([]error, requests)

	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := firstID + int64(i)
			errs[i] = d.Dispatch(context.Background(), id, fmt.Sprintf("acct%d", id), &sinks[i])
		}(i)
	}
	wg.Wait()
	d.Close()

	for i := 0; i < requests; i++ {
		id := firstID + int64(i)
		if errs[i] != nil {
			t.Errorf("account %d: dispatch failed: %v", id, errs[i])
			continue
		}

		got := strings.Fields(sinks[i].String())
		if len(got) != tokens {
			t.Errorf("account %d: got %d tokens, want %d: %q", id, len(got), tokens, sinks[i].String())
			continue
		}
		for j, tok := range got {
			want := fmt.Sprintf("acct%d-tok-%d", id, j)
			if tok != want {
				t.Errorf("account %d: token %d is %q, want %q", id, j, tok, want)
				break
			}
		}

		if n := d.Usage(id); n != tokens {
			t.Errorf("account %d: metered %d tokens, want %d", id, n, tokens)
		}
	}
}
