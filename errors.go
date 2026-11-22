package snodramessaging

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// Code is a protocol error code, with the numbers of the hub protocol. See
// https://snodra.damoreira.com/library-authors/messages/.
type Code int32

// The error codes a call can fail with.
const (
	CodeNoHandler           Code = 1
	CodeTimeout             Code = 2
	CodeHandlerDisconnected Code = 3
	CodeHandlerError        Code = 4
	CodeUnauthenticated     Code = 5
	CodeInvalidRequest      Code = 6
	CodePermissionDenied    Code = 7
	CodeResourceExhausted   Code = 8
	CodeCancelled           Code = 9
)

// String returns the protocol name of the code ("NO_HANDLER", ...).
func (c Code) String() string {
	if c >= CodeNoHandler && c <= CodeCancelled {
		return hubpb.ErrorCode(c).String()
	}
	return hubpb.ErrorCode_ERROR_CODE_UNSPECIFIED.String()
}

// Error is an error answered by the hub or by a remote handler. Details holds
// the handler's machine-readable details (JSON), only for CodeHandlerError.
// See https://snodra.damoreira.com/library-authors/messages/.
type Error struct {
	Code    Code
	Message string
	Details json.RawMessage
}

// Error returns "<CODE>: <Message>".
func (e *Error) Error() string { return e.Code.String() + ": " + e.Message }

// Errors the library itself reports, to be tested with errors.Is.
var (
	// ErrNeverSent: the request was not written to any hub; sending it again
	// is safe.
	ErrNeverSent = errors.New("snodra: the request was never sent")
	// ErrOutcomeUnknown: the request was written but no answer arrived; the
	// handler may or may not have run.
	ErrOutcomeUnknown = errors.New("snodra: the outcome of the request is unknown")
	// ErrLostParts: a stream or gather lost parts that were already announced.
	ErrLostParts = errors.New("snodra: parts of the answer were lost")
	// ErrReservedHeader: a header name is reserved for the library.
	ErrReservedHeader = errors.New("snodra: header name is reserved")
	// ErrRegistrationClosed: handlers can no longer be registered.
	ErrRegistrationClosed = errors.New("snodra: registration is closed")
)

// handlerError is an error a handler returns; it may carry a protocol code
// (set only by the constructors below) and details.
type handlerError struct {
	code    Code
	msg     string
	details json.RawMessage
	cause   error
}

func (e *handlerError) Error() string { return e.msg }
func (e *handlerError) Unwrap() error { return e.cause }

// Unauthenticated returns an error that a handler answers as UNAUTHENTICATED.
func Unauthenticated(msg string) error {
	return &handlerError{code: CodeUnauthenticated, msg: msg}
}

// PermissionDenied returns an error that a handler answers as
// PERMISSION_DENIED.
func PermissionDenied(msg string) error {
	return &handlerError{code: CodePermissionDenied, msg: msg}
}

// InvalidRequest returns an error that a handler answers as INVALID_REQUEST.
func InvalidRequest(msg string) error {
	return &handlerError{code: CodeInvalidRequest, msg: msg}
}

// WithDetails attaches details, marshalled with encoding/json, to err; the
// handler answers them as the error's details. A nil err stays nil.
func WithDetails(err error, details any) error {
	if err == nil {
		return nil
	}
	raw, merr := json.Marshal(details)
	if merr != nil {
		return err
	}
	return &handlerError{msg: err.Error(), details: raw, cause: err}
}

func errorFromWire(e *hubpb.Error) *Error {
	code := Code(e.GetCode())
	if code < CodeNoHandler || code > CodeCancelled {
		code = CodeHandlerError
	}
	out := &Error{Code: code, Message: e.GetMessage()}
	if code == CodeHandlerError && len(e.GetDetails()) > 0 {
		out.Details = json.RawMessage(append([]byte(nil), e.GetDetails()...))
	}
	return out
}

var lineBreaks = strings.NewReplacer("\r\n", " ", "\n", " ", "\r", " ")

// errorToWire is the Error a handler's err answers: the code of the nearest error made by
// Unauthenticated, PermissionDenied or InvalidRequest, else HANDLER_ERROR; the message on one
// line; the nearest details, only for HANDLER_ERROR (the protocol keeps details for that code
// only). A nil err, which no caller should pass, is a HANDLER_ERROR too.
func errorToWire(requestID string, err error) *hubpb.Error {
	if err == nil {
		return &hubpb.Error{RequestId: requestID, Code: hubpb.ErrorCode_HANDLER_ERROR,
			Message: "the handler failed without an error"}
	}
	w := &hubpb.Error{
		RequestId: requestID,
		Code:      hubpb.ErrorCode_HANDLER_ERROR,
		Message:   lineBreaks.Replace(err.Error()),
	}
	// The nearest marked error decides the code; the nearest details win.
	var he *handlerError
	for e := err; e != nil; {
		if !errors.As(e, &he) {
			break
		}
		if he.code != 0 && w.Code == hubpb.ErrorCode_HANDLER_ERROR {
			w.Code = hubpb.ErrorCode(he.code)
		}
		if he.details != nil && w.Details == nil {
			w.Details = he.details
		}
		e = he.cause
	}
	if w.Code != hubpb.ErrorCode_HANDLER_ERROR {
		w.Details = nil
	}
	return w
}
