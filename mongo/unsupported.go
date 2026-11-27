package mongo

import (
	"context"
	"fmt"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// The ports this level does not implement yet return snodra.ErrUnsupported: the external-call and
// saga stores (level 4). The interfaces are frozen at level 2 so a third-party add-on compiles
// once; the official add-ons implement every port by level 4.

func unsupported(what string) error {
	return fmt.Errorf("%w: the MongoDB message store does not implement %s yet", snodra.ErrUnsupported, what)
}

// ---- external calls -----------------------------------------------------------------------------

type unsupportedExternalCalls struct{}

var _ snodra.ExternalCallStore = unsupportedExternalCalls{}

func (unsupportedExternalCalls) Insert(context.Context, snodra.ExternalCall) (bool, error) {
	return false, unsupported("the external-call store")
}

func (unsupportedExternalCalls) Find(context.Context, string) (*snodra.ExternalCall, error) {
	return nil, unsupported("the external-call store")
}

func (unsupportedExternalCalls) TakeOver(context.Context, snodra.ExternalCall, string, time.Time, time.Time) (*snodra.ExternalCall, error) {
	return nil, unsupported("the external-call store")
}

func (unsupportedExternalCalls) Expire(context.Context, snodra.ExternalCall, time.Time) (bool, error) {
	return false, unsupported("the external-call store")
}

func (unsupportedExternalCalls) Succeed(context.Context, string, string, string, time.Time) (bool, error) {
	return false, unsupported("the external-call store")
}

func (unsupportedExternalCalls) Fail(context.Context, string, string, string, time.Time) (bool, error) {
	return false, unsupported("the external-call store")
}

func (unsupportedExternalCalls) MarkUnknown(context.Context, string, string, time.Time) (bool, error) {
	return false, unsupported("the external-call store")
}

func (unsupportedExternalCalls) Reset(context.Context, string, string) (bool, error) {
	return false, unsupported("the external-call store")
}

// ---- sagas --------------------------------------------------------------------------------------

type unsupportedSagas struct{}

var _ snodra.SagaRepository = unsupportedSagas{}

func (unsupportedSagas) FindByID(context.Context, string) (*snodra.Saga, error) {
	return nil, unsupported("the saga store")
}

func (unsupportedSagas) FindLive(context.Context, string, string, string) ([]snodra.Saga, error) {
	return nil, unsupported("the saga store")
}

func (unsupportedSagas) Insert(context.Context, snodra.Saga) error {
	return unsupported("the saga store")
}

func (unsupportedSagas) Update(context.Context, snodra.Saga, int64) (bool, error) {
	return false, unsupported("the saga store")
}

func (unsupportedSagas) CountLiveByType(context.Context) (map[string]int64, error) {
	return nil, unsupported("the saga store")
}

type unsupportedSagaDispatch struct{}

var _ snodra.SagaDispatchStore = unsupportedSagaDispatch{}

func (unsupportedSagaDispatch) Record(context.Context, []snodra.SagaDispatch) error {
	return unsupported("the saga dispatch store")
}

func (unsupportedSagaDispatch) DueSagas(context.Context, time.Time, []string, int) ([]string, error) {
	return nil, unsupported("the saga dispatch store")
}

func (unsupportedSagaDispatch) NextPending(context.Context, string) (*snodra.SagaDispatch, error) {
	return nil, unsupported("the saga dispatch store")
}

func (unsupportedSagaDispatch) MarkDispatched(context.Context, string, time.Time) error {
	return unsupported("the saga dispatch store")
}

func (unsupportedSagaDispatch) ScheduleRetry(context.Context, string, int, time.Time) error {
	return unsupported("the saga dispatch store")
}

func (unsupportedSagaDispatch) MarkFailed(context.Context, string, snodra.DispatchFailure) error {
	return unsupported("the saga dispatch store")
}

func (unsupportedSagaDispatch) ClaimDueNotice(context.Context, time.Time, time.Time) (*snodra.SagaDispatch, error) {
	return nil, unsupported("the saga dispatch store")
}

func (unsupportedSagaDispatch) ClearNotice(context.Context, string) error {
	return unsupported("the saga dispatch store")
}

func (unsupportedSagaDispatch) ScheduleNoticeRetry(context.Context, string, int, time.Time) error {
	return unsupported("the saga dispatch store")
}

func (unsupportedSagaDispatch) ClaimLease(context.Context, string, string, time.Time, time.Time) (bool, error) {
	return false, unsupported("the saga dispatch store")
}

func (unsupportedSagaDispatch) RenewLease(context.Context, string, string, time.Time) (bool, error) {
	return false, unsupported("the saga dispatch store")
}

func (unsupportedSagaDispatch) ReleaseLease(context.Context, string, string) error {
	return unsupported("the saga dispatch store")
}

func (unsupportedSagaDispatch) Pending(context.Context) (int64, error) {
	return 0, unsupported("the saga dispatch store")
}

type unsupportedSagaDeadlines struct{}

var _ snodra.SagaDeadlineStore = unsupportedSagaDeadlines{}

func (unsupportedSagaDeadlines) Insert(context.Context, snodra.SagaDeadline) error {
	return unsupported("the saga deadline store")
}

func (unsupportedSagaDeadlines) FindScheduled(context.Context, string) (*snodra.SagaDeadline, error) {
	return nil, unsupported("the saga deadline store")
}

func (unsupportedSagaDeadlines) Cancel(context.Context, string, string, time.Time) error {
	return unsupported("the saga deadline store")
}

func (unsupportedSagaDeadlines) CancelByName(context.Context, string, string, time.Time) error {
	return unsupported("the saga deadline store")
}

func (unsupportedSagaDeadlines) CancelAll(context.Context, string, time.Time) error {
	return unsupported("the saga deadline store")
}

func (unsupportedSagaDeadlines) MarkFired(context.Context, string, time.Time) (bool, error) {
	return false, unsupported("the saga deadline store")
}

func (unsupportedSagaDeadlines) ClaimDue(context.Context, string, time.Time, time.Time) (*snodra.SagaDeadline, error) {
	return nil, unsupported("the saga deadline store")
}

func (unsupportedSagaDeadlines) CountOverdueByName(context.Context, time.Time) (map[string]int64, error) {
	return nil, unsupported("the saga deadline store")
}

type unsupportedSagaFailures struct{}

var _ snodra.SagaFailureStore = unsupportedSagaFailures{}

func (unsupportedSagaFailures) Find(context.Context, string) (*snodra.SagaFailure, error) {
	return nil, unsupported("the saga failure store")
}

func (unsupportedSagaFailures) Save(context.Context, snodra.SagaFailure) error {
	return unsupported("the saga failure store")
}

func (unsupportedSagaFailures) Delete(context.Context, string) (bool, error) {
	return false, unsupported("the saga failure store")
}

func (unsupportedSagaFailures) FailedAgain(context.Context, string, string, int, time.Time) (bool, error) {
	return false, unsupported("the saga failure store")
}

func (unsupportedSagaFailures) List(context.Context, string) ([]snodra.SagaFailure, error) {
	return nil, unsupported("the saga failure store")
}

type unsupportedSagaMarkers struct{}

var _ snodra.SagaMarkerStore = unsupportedSagaMarkers{}

func (unsupportedSagaMarkers) Exists(context.Context, string) (bool, error) {
	return false, unsupported("the saga marker store")
}

func (unsupportedSagaMarkers) Insert(context.Context, string, string, time.Time) error {
	return unsupported("the saga marker store")
}
