package inferno

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultDatabaseURL = "postgres://inferno:inferno@localhost:5432/inferno?sslmode=disable"

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = defaultDatabaseURL
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, url)
	if err == nil {
		err = pool.Ping(ctx)
	}
	if err != nil {
		t.Fatalf("cannot connect to postgres at %s: %v\n\nstart the database first: docker compose up -d", url, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func seedAccount(t *testing.T, pool *pgxpool.Pool, id, credits int64) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO accounts (id, credits) VALUES ($1, $2)
		 ON CONFLICT (id) DO UPDATE SET credits = EXCLUDED.credits`, id, credits)
	if err != nil {
		t.Fatalf("seed account %d: %v", id, err)
	}
}

func accountCredits(t *testing.T, pool *pgxpool.Pool, id int64) int64 {
	t.Helper()
	var credits int64
	err := pool.QueryRow(context.Background(), `SELECT credits FROM accounts WHERE id = $1`, id).Scan(&credits)
	if err != nil {
		t.Fatalf("read account %d: %v", id, err)
	}
	return credits
}

type fakeNode struct {
	*httptest.Server
	tokens   int
	maxDelay time.Duration
	newConns atomic.Int64
}

func newFakeNode(t *testing.T, tokens int, maxDelay time.Duration) *fakeNode {
	t.Helper()
	n := &fakeNode{tokens: tokens, maxDelay: maxDelay}
	n.Server = httptest.NewUnstartedServer(http.HandlerFunc(n.handle))
	n.Server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			n.newConns.Add(1)
		}
	}
	n.Start()
	t.Cleanup(n.Close)
	return n
}

func (n *fakeNode) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/generate" {
		http.NotFound(w, r)
		return
	}

	var req struct {
		Prompt string `json:"prompt"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	flusher := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	for i := 0; i < n.tokens; i++ {
		if n.maxDelay > 0 {
			time.Sleep(rand.N(n.maxDelay))
		}
		fmt.Fprintf(w, "data: %s-tok-%d \n\n", req.Prompt, i)
		flusher.Flush()
	}

	fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()

	time.Sleep(time.Millisecond)
	fmt.Fprint(w, ": keep-alive\n\n")
}
