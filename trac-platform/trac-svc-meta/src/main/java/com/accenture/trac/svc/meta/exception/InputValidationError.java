package com.accenture.trac.svc.meta.exception;

public class InputValidationError extends TracError {

    public InputValidationError(String message) {
        super(message);
    }

    public InputValidationError(String message, Throwable cause) {
        super(message, cause);
    }
}
