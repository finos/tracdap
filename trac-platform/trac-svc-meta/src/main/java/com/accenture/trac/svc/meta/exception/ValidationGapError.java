package com.accenture.trac.svc.meta.exception;


/**
 * A validation gap error is a type of internal error, it indicates a condition
 * inside a TRAC component that should have been caught higher up the stack
 * in a validation layer.
 */
public class ValidationGapError extends TracInternalError {

    public ValidationGapError(String message, Throwable cause) {
        super(message, cause);
    }

    public ValidationGapError(String message) {
        super(message);
    }
}
