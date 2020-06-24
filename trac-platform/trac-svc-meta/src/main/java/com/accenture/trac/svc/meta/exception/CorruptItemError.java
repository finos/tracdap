package com.accenture.trac.svc.meta.exception;

public class CorruptItemError extends MetadataError {

    public CorruptItemError(String message) {
        super(message);
    }

    public CorruptItemError(String message, Throwable cause) {
        super(message, cause);
    }
}
