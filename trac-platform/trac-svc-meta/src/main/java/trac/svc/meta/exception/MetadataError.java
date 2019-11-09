package trac.svc.meta.exception;

public class MetadataError extends TracError {

    public MetadataError(String message) {
        super(message);
    }

    public MetadataError(String message, Throwable cause) {
        super(message, cause);
    }
}
