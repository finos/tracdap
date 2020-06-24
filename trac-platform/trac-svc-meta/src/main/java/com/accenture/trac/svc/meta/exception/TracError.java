package com.accenture.trac.svc.meta.exception;

public class TracError extends RuntimeException {

    public TracError(String message) {
        super(message);
    }

    public TracError(String message, Throwable cause) {
        super(message, cause);
    }
}
