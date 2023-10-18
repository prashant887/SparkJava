package common;

public enum ErrorType {

    ERROR_DB_INIT(1000, "Error initializing database session"),
    ERROR_EXCEED_MAXPART(1001, "Query result exceeded maximum number of partitions allowed"),
    ERROR_SET_INPUT(1002, "Error setting input information"),
    ERROR_INVALID_TABLE(2000, "Table specified does not exist"),
    ERROR_SET_OUTPUT(2001, "Error setting output information"),
    ERROR_DUPLICATE_PARTITION(2002, "Partition already present with given partition key values"),
    ERROR_NON_EMPTY_TABLE(2003, "Non-partitioned table already contains data"),
    ERROR_NOT_INITIALIZED(2004, "HCatOutputFormat not initialized, setOutput has to be called"),
    ERROR_INIT_STORAGE_HANDLER(2005, "Error initializing storage handler instance"),
    ERROR_PUBLISHING_PARTITION(2006, "Error adding partition to metastore"),
    ERROR_SCHEMA_COLUMN_MISMATCH(2007, "Invalid column position in partition schema"),
    ERROR_SCHEMA_PARTITION_KEY(2008, "Partition key cannot be present in the partition data"),
    ERROR_SCHEMA_TYPE_MISMATCH(2009, "Invalid column type in partition schema"),
    ERROR_INVALID_PARTITION_VALUES(2010, "Invalid partition values specified"),
    ERROR_MISSING_PARTITION_KEY(2011, "Partition key value not provided for publish"),
    ERROR_MOVE_FAILED(2012, "Moving of data failed during commit"),
    ERROR_TOO_MANY_DYNAMIC_PTNS(2013, "Attempt to create too many dynamic partitions"),
    ERROR_INIT_LOADER(2014, "Error initializing Pig loader"),
    ERROR_INIT_STORER(2015, "Error initializing Pig storer"),
    ERROR_NOT_SUPPORTED(2016, "Error operation not supported"),
    ERROR_ACCESS_CONTROL(3000, "Permission denied"),
    ERROR_UNIMPLEMENTED(9000, "Functionality currently unimplemented"),
    ERROR_INTERNAL_EXCEPTION(9001, "Exception occurred while processing HCat request");

    private int errorCode;
    private String errorMessage;
    private boolean appendCauseMessage = true;
    private boolean isRetriable = false;

    private ErrorType(int errorCode, String errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    private ErrorType(int errorCode, String errorMessage, boolean appendCauseMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.appendCauseMessage = appendCauseMessage;
    }

    private ErrorType(int errorCode, String errorMessage, boolean appendCauseMessage, boolean isRetriable) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.appendCauseMessage = appendCauseMessage;
        this.isRetriable = isRetriable;
    }

    public int getErrorCode() {
        return this.errorCode;
    }

    public String getErrorMessage() {
        return this.errorMessage;
    }

    public boolean isRetriable() {
        return this.isRetriable;
    }

    public boolean appendCauseMessage() {
        return this.appendCauseMessage;
    }
}
