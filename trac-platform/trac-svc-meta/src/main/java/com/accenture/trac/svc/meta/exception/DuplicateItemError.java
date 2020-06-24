package com.accenture.trac.svc.meta.exception;

public class DuplicateItemError extends MetadataError {

    public DuplicateItemError(String message) {
        super(message);
    }

    public DuplicateItemError(String message, Throwable cause) {
        super(message, cause);
    }
}
