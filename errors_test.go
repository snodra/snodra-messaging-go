package snodramessaging

import (
	"errors"
	"fmt"
	"testing"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

func TestEveryCodeReadsAsItself(t *testing.T) {
	for c := int32(1); c <= 9; c++ {
		e := errorFromWire(&hubpb.Error{Code: hubpb.ErrorCode(c), Message: "m", Details: []byte(`{"a":1}`)})
		is(t, e.Code, Code(c))
		is(t, e.Message, "m")
		is(t, string(e.Details), map[bool]string{true: `{"a":1}`, false: ""}[c == 4]) // details only for HANDLER_ERROR
	}
	is(t, errorFromWire(&hubpb.Error{Code: 0}).Code, CodeHandlerError)  // ERROR_CODE_UNSPECIFIED
	is(t, errorFromWire(&hubpb.Error{Code: 42}).Code, CodeHandlerError) // unknown code
}

func TestHandlerErrorsMapToTheirCodes(t *testing.T) {
	is(t, errorToWire("r-1", errors.New("out of stock")).GetCode(), hubpb.ErrorCode_HANDLER_ERROR)
	is(t, errorToWire("r-1", errors.New("out of stock")).GetMessage(), "out of stock")
	is(t, errorToWire("r-1", errors.New("a\nb\r\nc")).GetMessage(), "a b c") // never a line break
	is(t, errorToWire("r-1", fmt.Errorf("check: %w", PermissionDenied("orders-user required"))).GetCode(), hubpb.ErrorCode_PERMISSION_DENIED)
	is(t, errorToWire("r-1", Unauthenticated("no token")).GetCode(), hubpb.ErrorCode_UNAUTHENTICATED)
	is(t, errorToWire("r-1", InvalidRequest("bad sku")).GetCode(), hubpb.ErrorCode_INVALID_REQUEST)
	d := errorToWire("r-1", WithDetails(errors.New("invalid"), map[string]string{"field": "sku"}))
	is(t, string(d.GetDetails()), `{"field":"sku"}`)
	is(t, d.GetRequestId(), "r-1")
}

func TestAFailedNestedCallIsTheHandlersOwnError(t *testing.T) {
	nested := errorFromWire(&hubpb.Error{Code: hubpb.ErrorCode_NO_HANDLER, Message: "no instance"})
	w := errorToWire("r-1", fmt.Errorf("stock: %w", nested))
	is(t, w.GetCode(), hubpb.ErrorCode_HANDLER_ERROR) // never NO_HANDLER passed through
}

func TestCodeNamesAndErrorText(t *testing.T) {
	is(t, CodeNoHandler.String(), "NO_HANDLER")
	is(t, CodeCancelled.String(), "CANCELLED")
	is(t, (&Error{Code: CodeTimeout, Message: "slow"}).Error(), "TIMEOUT: slow")
}

func TestDetailsGoOnTheWireOnlyForHandlerError(t *testing.T) {
	d := map[string]string{"field": "sku"}
	is(t, string(errorToWire("r-1", WithDetails(errors.New("invalid"), d)).GetDetails()), `{"field":"sku"}`)
	for _, err := range []error{
		WithDetails(InvalidRequest("bad sku"), d),
		WithDetails(PermissionDenied("orders-user required"), d),
		InvalidRequest("bad sku"),
	} {
		w := errorToWire("r-1", err)
		is(t, w.GetCode() != hubpb.ErrorCode_HANDLER_ERROR, true)
		is(t, len(w.GetDetails()), 0)
	}
}

func TestErrorToWireOfNilIsAHandlerError(t *testing.T) {
	w := errorToWire("r-1", nil)
	is(t, w.GetCode(), hubpb.ErrorCode_HANDLER_ERROR)
	is(t, w.GetRequestId(), "r-1")
	is(t, w.GetMessage() != "", true)
}
