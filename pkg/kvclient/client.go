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

func (c *Client) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	return c.mutate(ctx, http.MethodPut, key, value)
}

func (c *Client) Delete(ctx context.Context, key string) (uint64, error) {
	return c.mutate(ctx, http.MethodDelete, key, nil)
}

func (c *Client) mutate(ctx context.Context, method, key string, data []byte) (uint64, error) {
	body, err := c.do(ctx, method, "/v1/kv/"+url.PathEscape(key), data)
	if err != nil {
		return 0, err
	}

	var result struct {
		Index uint64 `json:"index"`
	}

	err = json.Unmarshal(body, &result)
	return result.Index, err
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	return c.do(ctx, http.MethodGet, "/v1/kv/"+url.PathEscape(key), nil)
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

	req.Header.Set("Content-Type", "application/octet-stream")
	response, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed (write outcome may be unknown): %w", err)
	}

	defer func() { _ = response.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		return nil, err
	}

	if len(body) > 1<<20 {
		return nil, fmt.Errorf("response too large")
	}

	if response.StatusCode != http.StatusOK {
		var detail struct {
			Error string `json:"error"`
		}

		if json.Unmarshal(body, &detail) != nil || detail.Error == "" {
			detail.Error = response.Status
		}

		return nil, &Error{StatusCode: response.StatusCode, Message: detail.Error}
	}

	return body, nil
}
