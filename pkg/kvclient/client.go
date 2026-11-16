// Package kvclient is a small REST client with replayable, bounded redirects.
package kvclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	endpoint string
	http     *http.Client
}

type Status struct {
	ID           string `json:"id"`
	Role         string `json:"role"`
	Term         uint64 `json:"term"`
	LeaderID     string `json:"leader_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	CommitIndex  uint64 `json:"commit_index"`
	AppliedIndex uint64 `json:"applied_index"`
	Available    bool   `json:"available"`
}

type Error struct {
	StatusCode int
	Message    string
}

func (e *Error) Error() string { return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Message) }

func New(endpoint string, timeout time.Duration) (*Client, error) {
	return NewWithTransport(endpoint, timeout, http.DefaultTransport)
}

// NewWithTransport allows callers to size connection pools or instrument HTTP.
// The caller owns the transport and is responsible for closing idle connections.
func NewWithTransport(endpoint string, timeout time.Duration, transport http.RoundTripper) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, fmt.Errorf("endpoint must be an http(s) origin, for example http://localhost:18001")
	}

	if timeout <= 0 {
		return nil, fmt.Errorf("timeout must be positive")
	}

	return &Client{
		endpoint: strings.TrimSuffix(endpoint, "/"),
		http: &http.Client{
			Timeout:   timeout,
			Transport: transport,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 5 {
					return fmt.Errorf("too many leader redirects")
				}

				if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
					return fmt.Errorf("invalid redirect scheme")
				}

				if req.Method != via[0].Method {
					return fmt.Errorf("redirect changed request method")
				}

				return nil
			},
		},
	}, nil
}

type Record struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

// Put accepts a JSON-encoded value, preserving its type.
func (c *Client) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	data, err := json.Marshal(Record{Key: key, Value: value})
	if err != nil {
		return 0, err
	}
	body, err := c.do(ctx, http.MethodPost, "/query", data)
	if err != nil {
		return 0, err
	}
	var result struct {
		Index uint64 `json:"index"`
	}
	_ = json.Unmarshal(body, &result)
	return result.Index, nil
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	body, err := c.do(ctx, http.MethodGet, "/query?key="+url.QueryEscape(key), nil)
	if err != nil {
		return nil, err
	}
	var result Record
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	if result.Key != key || !json.Valid(result.Value) {
		return nil, fmt.Errorf("invalid query response")
	}
	return result.Value, nil
}

func (c *Client) Log(ctx context.Context) ([]Record, error) {
	body, err := c.do(ctx, http.MethodGet, "/log", nil)
	if err != nil {
		return nil, err
	}
	var records []Record
	err = json.Unmarshal(body, &records)
	return records, err
}

func (c *Client) Status(ctx context.Context) (Status, error) {
	body, err := c.do(ctx, http.MethodGet, "/v1/status", nil)
	if err != nil {
		return Status{}, err
	}

	var state Status
	err = json.Unmarshal(body, &state)
	return state, err
}

func (c *Client) do(ctx context.Context, method, path string, data []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed (write outcome may be unknown): %w", err)
	}

	defer func() { _ = response.Body.Close() }()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		var detail struct {
			Error  string          `json:"error"`
			Detail json.RawMessage `json:"detail"`
		}

		if json.Unmarshal(body, &detail) != nil || detail.Error == "" {
			if len(detail.Detail) > 0 {
				detail.Error = string(detail.Detail)
			} else {
				detail.Error = response.Status
			}
		}

		return nil, &Error{StatusCode: response.StatusCode, Message: detail.Error}
	}

	return body, nil
}
