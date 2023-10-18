package common;

import java.io.IOException;

public class HCatException extends IOException {
    private static final long serialVersionUID = 1L;
    private final ErrorType errorType;

    public HCatException(ErrorType errorType) {
        this(errorType, (String)null, (Throwable)null);
    }

    public HCatException(ErrorType errorType, Throwable cause) {
        this(errorType, (String)null, cause);
    }

    public HCatException(ErrorType errorType, String extraMessage) {
        this(errorType, extraMessage, (Throwable)null);
    }

    public HCatException(ErrorType errorType, String extraMessage, Throwable cause) {
        super(buildErrorMessage(errorType, extraMessage, cause), cause);
        this.errorType = errorType;
    }

    public HCatException(String message) {
        this(ErrorType.ERROR_INTERNAL_EXCEPTION, message, (Throwable)null);
    }

    public HCatException(String message, Throwable cause) {
        this(ErrorType.ERROR_INTERNAL_EXCEPTION, message, cause);
    }

    public static String buildErrorMessage(ErrorType type, String extraMessage, Throwable cause) {
        StringBuilder message = new StringBuilder(HCatException.class.getName());
        message.append(" : " + type.getErrorCode());
        message.append(" : " + type.getErrorMessage());
        if (extraMessage != null) {
            message.append(" : " + extraMessage);
        }

        if (type.appendCauseMessage() && cause != null) {
            message.append(". Cause : " + cause.toString());
        }

        return message.toString();
    }

    public boolean isRetriable() {
        return this.errorType.isRetriable();
    }

    public ErrorType getErrorType() {
        return this.errorType;
    }

    public int getErrorCode() {
        return this.errorType.getErrorCode();
    }

    public String toString() {
        return this.getMessage();
    }
}
