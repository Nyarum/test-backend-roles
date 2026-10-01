package inferno

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type generateRequest struct {
	Prompt string `json:"prompt"`
}

func (d *Dispatcher) generate(ctx context.Context, prompt string) (*http.Response, error) {
	body, err := json.Marshal(generateRequest{Prompt: prompt})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.nodeURL+"/generate", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("node returned %s", resp.Status)
	}

	return resp, nil
}
