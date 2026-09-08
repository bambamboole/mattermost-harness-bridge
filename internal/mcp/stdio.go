// Package mcp is a deliberately tiny MCP server over stdio: enough to expose
// one tool to the Claude CLI as its --permission-prompt-tool. Line-delimited
// JSON-RPC 2.0, the subset of methods the CLI actually calls.
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

const protocolVersion = "2024-11-05"

type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

// Handler serves a tools/call for the named tool and returns the text
// content the CLI receives. isError marks the result as a tool error.
type Handler func(ctx context.Context, args json.RawMessage) (text string, isError bool, err error)

type Server struct {
	Name     string
	Version  string
	Tools    []Tool
	Handlers map[string]Handler
	// Log receives one line per unusual event; nil disables it.
	Log func(format string, args ...any)
}

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Serve reads requests from r until EOF and writes responses to w. Calls are
// handled concurrently so a slow approval does not block pings.
func (s *Server) Serve(ctx context.Context, r io.Reader, w io.Writer) error {
	var wmu sync.Mutex
	write := func(v any) {
		b, err := json.Marshal(v)
		if err != nil {
			s.logf("marshal response: %v", err)
			return
		}
		wmu.Lock()
		defer wmu.Unlock()
		w.Write(append(b, '\n'))
	}

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 16<<20)
	var wg sync.WaitGroup
	defer wg.Wait()
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			s.logf("bad request line: %v", err)
			continue
		}
		if len(req.ID) == 0 || string(req.ID) == "null" {
			// Notification: nothing to answer.
			continue
		}
		wg.Add(1)
		go func(req request) {
			defer wg.Done()
			res, rpcErr := s.dispatch(ctx, req)
			write(response{JSONRPC: "2.0", ID: req.ID, Result: res, Error: rpcErr})
		}(req)
	}
	return sc.Err()
}

func (s *Server) dispatch(ctx context.Context, req request) (any, *rpcError) {
	switch req.Method {
	case "initialize":
		return map[string]any{
			"protocolVersion": protocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": s.Name, "version": s.Version},
		}, nil
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		tools := s.Tools
		if tools == nil {
			tools = []Tool{}
		}
		return map[string]any{"tools": tools}, nil
	case "tools/call":
		var p struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return nil, &rpcError{Code: -32602, Message: "invalid params"}
		}
		h, ok := s.Handlers[p.Name]
		if !ok {
			return nil, &rpcError{Code: -32602, Message: fmt.Sprintf("unknown tool %q", p.Name)}
		}
		text, isError, err := h(ctx, p.Arguments)
		if err != nil {
			return nil, &rpcError{Code: -32000, Message: err.Error()}
		}
		return map[string]any{
			"content": []map[string]any{{"type": "text", "text": text}},
			"isError": isError,
		}, nil
	default:
		return nil, &rpcError{Code: -32601, Message: "method not found: " + req.Method}
	}
}

func (s *Server) logf(format string, args ...any) {
	if s.Log != nil {
		s.Log(format, args...)
	}
}
