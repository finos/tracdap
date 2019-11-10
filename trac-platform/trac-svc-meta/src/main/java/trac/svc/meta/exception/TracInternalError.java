package trac.svc.meta.exception;

public class TracInternalError extends RuntimeException {

    public TracInternalError(String message) {
        super(message);
    }

    public TracInternalError(String message, Throwable cause) {
        super(message, cause);
    }
}
