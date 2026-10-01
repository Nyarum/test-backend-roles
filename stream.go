package inferno

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"strings"
)

const sinkQueue = 32

func (d *Dispatcher) stream(accountID int64, resp *http.Response, sink io.Writer) error {
	defer resp.Body.Close()

	out := make(chan []byte, sinkQueue)
	written := make(chan error, 1)
	go func() {
		written <- writeChunks(sink, out)
	}()

	err := d.readEvents(accountID, resp.Body, out)
	close(out)
	if werr := <-written; err == nil {
		err = werr
	}
	return err
}

func (d *Dispatcher) readEvents(accountID int64, body io.Reader, out chan<- []byte) error {
	sc := bufio.NewScanner(body)
	var data []string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if len(data) == 0 {
				continue
			}
			if len(data) == 1 && data[0] == "[DONE]" {
				return nil
			}
			d.emit(accountID, data, out)
			data = data[:0]
		case strings.HasPrefix(line, "data:"):
			data = append(data, strings.TrimPrefix(line[len("data:"):], " "))
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return io.ErrUnexpectedEOF
}

func (d *Dispatcher) emit(accountID int64, data []string, out chan<- []byte) {
	buf := d.bufs.Get().(*bytes.Buffer)
	defer d.bufs.Put(buf)

	buf.Reset()
	for i, s := range data {
		if i > 0 {
			buf.WriteByte('\n')
		}
		buf.WriteString(s)
	}

	chunk := buf.Bytes()
	out <- chunk
	d.meter.Record(accountID, chunk)
}

func writeChunks(w io.Writer, chunks <-chan []byte) error {
	var err error
	for chunk := range chunks {
		if err != nil {
			continue
		}
		_, err = w.Write(chunk)
	}
	return err
}
