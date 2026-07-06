// Package protocol defines the Stratum V1 JSON-RPC message shapes and a
// newline-delimited codec. It contains no networking and no pool logic, which
// keeps it trivially unit-testable.
package protocol

import "encoding/json"

// Method names understood by the server.
const (
	MethodSubscribe     = "mining.subscribe"
	MethodAuthorize     = "mining.authorize"
	MethodConfigure     = "mining.configure"
	MethodSubmit        = "mining.submit"
	MethodGetVersion    = "client.get_version"
	MethodSetDifficulty = "mining.set_difficulty"
	MethodNotify        = "mining.notify"
	MethodReconnect     = "client.reconnect"
)

// Request is an inbound JSON-RPC request from a miner. Params are left raw so
// each handler can decode them into the exact positional shape it expects.
type Request struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

// Response is a JSON-RPC response to a miner request. Exactly one of Result or
// Error is meaningful; the Stratum convention is result=null with a populated
// error on failure.
type Response struct {
	ID     json.RawMessage `json:"id"`
	Result any             `json:"result"`
	Error  *RPCError       `json:"error"`
}

// Notification is a server-initiated message (no id) such as mining.notify or
// mining.set_difficulty.
type Notification struct {
	ID     any    `json:"id"` // always null on the wire
	Method string `json:"method"`
	Params any    `json:"params"`
}

// RPCError is the Stratum V1 error triple [code, message, traceback]. It
// marshals to a 3-element array to match the miners' expectations, not to a
// JSON object.
type RPCError struct {
	Code    int
	Message string
	Data    any
}

// MarshalJSON renders the error as the [code, message, data] array form.
func (e *RPCError) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{e.Code, e.Message, e.Data})
}

// UnmarshalJSON accepts the array form.
func (e *RPCError) UnmarshalJSON(b []byte) error {
	var arr []json.RawMessage
	if err := json.Unmarshal(b, &arr); err != nil {
		return err
	}
	if len(arr) > 0 {
		_ = json.Unmarshal(arr[0], &e.Code)
	}
	if len(arr) > 1 {
		_ = json.Unmarshal(arr[1], &e.Message)
	}
	if len(arr) > 2 {
		_ = json.Unmarshal(arr[2], &e.Data)
	}
	return nil
}

// Standard Stratum error codes.
const (
	ErrOther          = 20
	ErrJobNotFound    = 21
	ErrDuplicateShare = 22
	ErrLowDifficulty  = 23
	ErrUnauthorized   = 24
	ErrNotSubscribed  = 25
)

// NewError builds an RPCError with a nil traceback.
func NewError(code int, msg string) *RPCError {
	return &RPCError{Code: code, Message: msg, Data: nil}
}

// OKResponse builds a success response for the given id and result.
func OKResponse(id json.RawMessage, result any) *Response {
	return &Response{ID: id, Result: result, Error: nil}
}

// ErrResponse builds a failure response (result null, error populated).
func ErrResponse(id json.RawMessage, rpcErr *RPCError) *Response {
	return &Response{ID: id, Result: nil, Error: rpcErr}
}
