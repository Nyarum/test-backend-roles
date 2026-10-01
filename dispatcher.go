package inferno

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	NodeURL         string
	PricePerRequest int64
	RequestTimeout  time.Duration
}

type Dispatcher struct {
	db               *pgxpool.Pool
	nodeURL          string
	price            int64
	timeout          time.Duration
	bufs             sync.Pool
	meter            *Meter
	mt               *sync.Mutex
	client           *http.Client
	limitConnections chan struct{}
}

func New(db *pgxpool.Pool, cfg Config) *Dispatcher {
	timeout := cfg.RequestTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	d := &Dispatcher{
		db:      db,
		nodeURL: strings.TrimSuffix(cfg.NodeURL, "/"),
		price:   cfg.PricePerRequest,
		timeout: timeout,
		meter:   NewMeter(1024),
		mt:      &sync.Mutex{},
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				TLSHandshakeTimeout:   5 * time.Second,
				ResponseHeaderTimeout: timeout,
				IdleConnTimeout:       90 * time.Second,
			},
		},
		limitConnections: make(chan struct{}, 1),
	}
	d.bufs.New = func() any {
		return new(bytes.Buffer)
	}
	return d
}

func (d *Dispatcher) Dispatch(ctx context.Context, accountID int64, prompt string, sink io.Writer) error {
	d.limitConnections <- struct{}{}
	defer func() {
		<-d.limitConnections
	}()

	if err := d.Charge(ctx, accountID, d.price); err != nil {
		return fmt.Errorf("charge account %d: %w", accountID, err)
	}

	resp, err := d.generate(ctx, prompt)
	if err != nil {
		return fmt.Errorf("generate: %w", err)
	}

	if err := d.stream(accountID, resp, sink); err != nil {
		return fmt.Errorf("stream: %w", err)
	}

	return nil
}

func (d *Dispatcher) Usage(accountID int64) int64 {
	return d.meter.Usage(accountID)
}

func (d *Dispatcher) Close() {
	d.meter.Close()
}
