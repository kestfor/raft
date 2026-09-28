package raft

import "errors"

var (
	ErrNotRunning     = errors.New("raft: node is not running")
	ErrAlreadyStarted = errors.New("raft: node can only run once")
	ErrInvalidRequest = errors.New("raft: invalid request")
	ErrTermExhausted  = errors.New("raft: term counter exhausted")
)
