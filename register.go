package snodramessaging

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
)

// handlerConfig is what the [HandlerOption]s of a registration set.
type handlerConfig struct {
	// guards must all accept the caller before the payload is decoded; none: any caller.
	guards []func(Caller) bool
	// err is an option's error, returned by the registration.
	err error
}

// A HandlerOption configures one handler in [HandleCommand] or [HandleQuery]: its guards
// ([RequireAuthenticated], [RequireRole], [RequireCaller]).
type HandlerOption func(*handlerConfig)

// handlerKind is the request kind a handler serves.
type handlerKind int

const (
	kindCommand handlerKind = iota
	kindQuery
)

func (k handlerKind) String() string { return [...]string{"command", "query"}[k] }

// invoker runs a handler on a payload: decoded with the codec, encoded back. A payload that
// does not decode is a *decodeError.
type invoker func(ctx context.Context, codec PayloadCodec, payload []byte) ([]byte, error)

// handler is a registered handler of one type.
type handler struct {
	kind   handlerKind
	name   string
	invoke invoker
	cfg    handlerConfig
}

// decodeError is a payload the codec could not decode into the handler's message type.
type decodeError struct{ err error }

func (e *decodeError) Error() string { return "cannot decode payload: " + e.err.Error() }
func (e *decodeError) Unwrap() error { return e.err }

// HandleCommand registers h as the handler of the command type C, by C's MessageType. C may be
// a struct type or a pointer to one ([Message] may have a pointer receiver); the payload is
// decoded into a fresh C. R is the answer: [Empty] (or a nil pointer) answers {}.
//
// Handlers are registered before [App.Start]. A registration error (after Start:
// [ErrRegistrationClosed]; a type registered twice, as a command and a query, an empty type
// name, an interface type parameter, a nil handler, a [RequireCaller] without a function) is returned, and also kept: Start returns
// every one of them, joined, and does not start.
// See https://snodra.damoreira.com/library-authors/protocol/ (Handling a request).
func HandleCommand[C Message, R any](app *App, h func(context.Context, C) (R, error), opts ...HandlerOption) error {
	return register(app, kindCommand, h, opts)
}

// HandleQuery registers h as the handler of the query type Q, by Q's MessageType, as
// [HandleCommand] does for a command.
func HandleQuery[Q Message, R any](app *App, h func(context.Context, Q) (R, error), opts ...HandlerOption) error {
	return register(app, kindQuery, h, opts)
}

func register[M Message, R any](app *App, kind handlerKind, h func(context.Context, M) (R, error), opts []HandlerOption) error {
	if app == nil {
		return errors.New("snodra: registration on a nil App")
	}
	name, err := messageTypeOf[M]()
	if err == nil && h == nil {
		err = fmt.Errorf("snodra: nil handler for %s", name)
	}
	var cfg handlerConfig
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	if err == nil && cfg.err != nil {
		err = fmt.Errorf("%w (%s)", cfg.err, name)
	}
	return app.addHandler(&handler{kind: kind, name: name, invoke: invokerOf(h), cfg: cfg}, err)
}

// messageTypeOf is the wire name of M, or why M cannot be registered.
func messageTypeOf[M Message]() (name string, err error) {
	t := reflect.TypeFor[M]()
	if t.Kind() == reflect.Interface {
		return "", fmt.Errorf("snodra: %v is an interface type: register a concrete message type", t)
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("snodra: %v.MessageType panicked: %v", t, r)
		}
	}()
	name = typeName[M]()
	if name == "" {
		return "", fmt.Errorf("snodra: %v has an empty type name (MessageType returned \"\")", t)
	}
	return name, nil
}

// invokerOf adapts h to the dispatcher: decode a fresh M, run h, encode its answer.
func invokerOf[M Message, R any](h func(context.Context, M) (R, error)) invoker {
	return func(ctx context.Context, codec PayloadCodec, payload []byte) ([]byte, error) {
		msg := newValue[M]()
		var target any = &msg
		if t := reflect.TypeFor[M](); t.Kind() == reflect.Pointer {
			target = msg // already a fresh pointer: decode into what it points to
		}
		if err := codec.Unmarshal(payload, target); err != nil {
			return nil, &decodeError{err}
		}
		result, err := h(ctx, msg)
		if err != nil {
			return nil, err
		}
		return encodeResult(codec, result)
	}
}

// encodeResult encodes a handler's answer; Empty, a nil pointer and a nil interface are {}.
func encodeResult[R any](codec PayloadCodec, r R) ([]byte, error) {
	switch v := reflect.ValueOf(&r).Elem(); {
	case v.Kind() == reflect.Interface && v.IsNil(), v.Kind() == reflect.Pointer && v.IsNil():
		return []byte("{}"), nil
	}
	if _, ok := any(r).(Empty); ok {
		return []byte("{}"), nil
	}
	b, err := codec.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("cannot encode the answer: %w", err)
	}
	return b, nil
}

// addHandler records h, or the registration error err (or the one h makes).
func (a *App) addHandler(h *handler, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err == nil {
		err = a.checkHandler(h)
	}
	if err != nil {
		a.regErrs = append(a.regErrs, err)
		return err
	}
	a.handlers[h.name] = h
	return nil
}

func (a *App) checkHandler(h *handler) error {
	if a.started || a.stopping {
		return fmt.Errorf("%w: %s %s registered after Start", ErrRegistrationClosed, h.kind, h.name)
	}
	if prev, ok := a.handlers[h.name]; ok {
		if prev.kind != h.kind {
			return fmt.Errorf("snodra: %s is registered both as a command and as a query", h.name)
		}
		return fmt.Errorf("snodra: %s %s is already registered", h.kind, h.name)
	}
	return nil
}

// registrationError is every registration error so far, joined; nil when none. Called with
// a.mu held.
func (a *App) registrationError() error {
	return errors.Join(a.regErrs...)
}

// freezeTypes lists the registered types for the Register, sorted. Called by Start with a.mu
// held; the handlers do not change afterwards.
func (a *App) freezeTypes() {
	a.commandTypes, a.queryTypes = nil, nil
	for name, h := range a.handlers {
		switch h.kind {
		case kindCommand:
			a.commandTypes = append(a.commandTypes, name)
		case kindQuery:
			a.queryTypes = append(a.queryTypes, name)
		}
	}
	slices.Sort(a.commandTypes)
	slices.Sort(a.queryTypes)
}
