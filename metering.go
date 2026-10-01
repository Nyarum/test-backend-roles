package inferno

import (
	"bytes"
	"sync"
)

type usage struct {
	accountID int64
	chunk     []byte
}

type Meter struct {
	in   chan usage
	done chan struct{}

	mu     sync.Mutex
	tokens map[int64]int64
}

func NewMeter(buffer int) *Meter {
	m := &Meter{
		in:     make(chan usage, buffer),
		done:   make(chan struct{}),
		tokens: make(map[int64]int64),
	}
	go m.run()
	return m
}

func (m *Meter) Record(accountID int64, chunk []byte) {
	m.in <- usage{accountID: accountID, chunk: chunk}
}

func (m *Meter) Usage(accountID int64) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tokens[accountID]
}

func (m *Meter) Close() {
	close(m.in)
	<-m.done
}

func (m *Meter) run() {
	defer close(m.done)
	for u := range m.in {
		n := int64(len(bytes.Fields(u.chunk)))
		m.mu.Lock()
		m.tokens[u.accountID] += n
		m.mu.Unlock()
	}
}
