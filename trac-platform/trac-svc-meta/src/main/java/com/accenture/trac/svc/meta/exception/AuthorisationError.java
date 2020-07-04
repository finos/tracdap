package com.accenture.trac.svc.meta.exception;

public class AuthorisationError extends TracError {

    public AuthorisationError(String message) {
        super(message);
    }

    public AuthorisationError(String message, Throwable cause) {
        super(message, cause);
    }
}
