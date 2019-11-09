package trac.svc.meta.exception;

public class MissingItemError extends MetadataError {

    public MissingItemError(String message) {
        super(message);
    }

    public MissingItemError(String message, Throwable cause) {
        super(message, cause);
    }
}
