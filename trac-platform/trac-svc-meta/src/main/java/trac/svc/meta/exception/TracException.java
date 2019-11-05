package trac.svc.meta.exception;

public class TracException extends RuntimeException {

    public TracException(String message) {
        super(message);
    }

    public TracException(String message, Throwable cause) {
        super(message, cause);
    }
}
