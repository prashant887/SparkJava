package etl;

public interface EtlError {

    /**
     * @return the error code to be used when populating error records thus allowing easier querying and filtering of
     *         different error records
     */
    String getErrorCode();

    /**
     * @return optional error type identifier that further specifies the error code; when translating exceptions to
     *         {@link EtlError}s, this is usually the simple type name of the root cause
     */
    String getErrorSubType();

    /**
     * @return the actual error message
     */
    String getErrorMessage();
}
