package trac.svc.meta.exception;

public class WrongItemTypeError extends MetadataError {

    public WrongItemTypeError(String message) {
        super(message);
    }

    public WrongItemTypeError(String message, Throwable cause) {
        super(message, cause);
    }
}
