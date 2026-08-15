// Package service implements the QuotaRaft quota ledger: the mutating
// operations (deduct, reserve, commit, rollback), the maintenance coordinator
// (expiry, refill, cycle reset), and recovery/reconciliation.
//
// Every mutating operation runs inside a single Store transaction. Within that
// transaction the service settles token-bucket refills, checks availability
// across all resource dimensions, writes immutable ledger entries, updates the
// balance projection, and records the idempotency response. Because the whole
// sequence is atomic, a failure at any point rolls everything back: there is
// never a partial deduction, a half-frozen reservation, or a ledger entry
// without its corresponding projection update.
package service

import (
	"errors"
	"fmt"
)

// Code is a stable, machine-readable error code.
type Code string

const (
	// CodeInsufficientQuota means one or more resource dimensions did not have
	// enough available quota.
	CodeInsufficientQuota Code = "INSUFFICIENT_QUOTA"
	// CodeIdempotencyConflict means an idempotency key was reused with a
	// different request body.
	CodeIdempotencyConflict Code = "IDEMPOTENCY_CONFLICT"
	// CodePolicyVersionConflict means the request expected a policy version
	// that does not match the current version.
	CodePolicyVersionConflict Code = "POLICY_VERSION_CONFLICT"
	// CodeReservationTerminated means the reservation is already in a terminal
	// state and cannot transition again.
	CodeReservationTerminated Code = "RESERVATION_TERMINATED"
	// CodeInputOverflow means a numeric input would overflow int64.
	CodeInputOverflow Code = "INPUT_OVERFLOW"
	// CodeInvalidInput means the request was malformed.
	CodeInvalidInput Code = "INVALID_INPUT"
	// CodeNotFound means a referenced entity does not exist.
	CodeNotFound Code = "NOT_FOUND"
	// CodeReadonly means the service is in a read-only fault state.
	CodeReadonly Code = "READONLY"
	// CodeInternal means an unexpected internal error occurred.
	CodeInternal Code = "INTERNAL"
)

// Error is a typed service error carrying a stable code.
type Error struct {
	Code    Code
	Message string
	Cause   error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Cause }

// NewError constructs a typed error.
func NewError(code Code, msg string) *Error {
	return &Error{Code: code, Message: msg}
}

// WrapError constructs a typed error wrapping a cause.
func WrapError(code Code, msg string, cause error) *Error {
	return &Error{Code: code, Message: msg, Cause: cause}
}

// AsCode extracts the Code from an error, defaulting to CodeInternal.
func AsCode(err error) Code {
	var se *Error
	if errors.As(err, &se) {
		return se.Code
	}
	return CodeInternal
}

// IsRetryable reports whether an error represents a transient failure (busy)
// that a client may retry. Service-level business errors are not retryable.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var se *Error
	if errors.As(err, &se) {
		return false
	}
	// Non-service errors (busy, context) are potentially retryable.
	return true
}
