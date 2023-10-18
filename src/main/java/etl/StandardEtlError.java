package etl;

public enum StandardEtlError implements EtlError {

    BundleParseFailure("50", "Bundle parsing failed"),
    RuntimeError("51", "Runtime error during ETL"),
    MappingMissing("100", "Column not found in table schema. Please promote column."),
    MappingIdNotPk("101", "Mapping of ID column must be a primary key. Please write a service ticket."),
    TransformationFailed("102", "Transformation of column failed. Send the correct data format."),
    MappingPkCannotBeNull("103", "ID cannot be null or an empty string. Please populate the @id field in your objects."),
    RecordMetadataMissing("105", "Table schema does not exist. Please promote table."),
    BootstrapThresholdExceeded("106", "Bootstrapping threshold exceeded. Reduce amount of new columns or write a service ticket."),
    InvalidRecordType("700", "Type/table transforms into an invalid table name. Change type/table value or write a service ticket."),
    LoadToViewError("701", "Type/table is a view, which cannot be directly written to. Send data to the table(s) which make up this view."),
    CdfJsonParsingFailure("800", "CDF Json parsing failure"),
    CdfArrayJsonParsingFailed("801", "CDF-with-json-array parsing failure"),
    CdfJsonForFlatteningParsingFailure("802", "CDF Json for flattening parsing failure"),
    DataFormatError("803", "Unexpected data format received. Please examine the data with the Ingestion Inspector.")
    //key 900 is used by com.vmware.ph.analytics.mddrepo.error.CustomMetadataError
    ;



    StandardEtlError(String code, String message) {
        this.code = code;
        this.message = message;
    }

    private final String code;
    private final String message;

    @Override
    public String getErrorCode() {
        return this.code;
    }

    @Override
    public String getErrorMessage() {
        return this.message;
    }

    @Override
    public String getErrorSubType() {
        return "";
    }
}
