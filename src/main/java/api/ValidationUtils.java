package api;

/**
 * Provides some utility methods for validating arguments and objects
 */
public class ValidationUtils {
    /**
     * @param string the passed string to validate
     * @param message message to pass to the exception if the validation fails
     */
    public static void validateNonBlank(String string, String message) {
        if (string == null || string.trim().isEmpty()) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Throw {@link IllegalArgumentException} if the object is null
     * @param object object that is validated
     * @param message message to pass to the exception if the validation fails
     */
    public static void validateNotNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }
}
