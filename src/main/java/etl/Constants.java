package etl;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public interface Constants {

    public static final List<String> PROTOTYPING_COLLECTORS =
            Arrays.asList("prototyping-only.v0", "prototyping-streaming.v0");

    //if you are adding a new format you should also add support for it in /productanalytics/analytics/schema/mdd/MDD_Postgres_DDL_Scripts.sql
    public static final String FLATTENING_FORMAT = "json-ld-for-flattening";
    public static final String CDF_FORMAT = "cdf";
    public static final String CDF_FORMAT_WITH_ARRAY = "cdf-with-json-array";
    public static final String NO_OP_FORMAT = "no-op";
    public static final String STREAMING_FORMAT = "json-cdf";
    /**
     * The default content format during the Metadata ETL process if none is specified.
     */
    public static final String DEFAULT_CONTENT_FORMAT = FLATTENING_FORMAT;

    /**
     * The standard suffix for foreign key columns. The naming convention for columns that are foreign keys to other
     * tables is <code> <table name> + <this suffix> </code>. For example "VC__FK", "ESX__FK", etc.
     *
     * It is not enforced by the Analytics platform, but can be used to infer the name of the referenced table if it is
     * not known.
     */
    public static final String FOREIGN_KEY_COLUMN_SUFFIX = "FK";

    public static final String NAMESPACE_SEPARATOR = "__";

    public static final String BUNDLE_TABLE = "BUNDLE";
    public static final String BUNDLE_DEBUG_INFO_TABLE = "BUNDLE_DEBUG_INFO";
    public static final String COLLECTION_TABLE = "COLLECTION";
    public static final String CHUNK_TABLE = "CHUNK";

    public static final String ERROR_RECORDS_TABLE = "ETL_ERROR_RECORD";
    public static final String ERROR_RECORDS_SOURCE_ENTITY = "source_entity";
    public static final String ERROR_RECORDS_SOURCE_RAW_DATA = "source_raw_data";
    public static final String ERROR_RECORDS_ERROR_SUBTYPE = "error_subtype";
    public static final String ERROR_RECORDS_ERROR_CODE = "error_code";
    public static final String ERROR_RECORDS_ERROR_MESSAGE = "error_message";

    public static final List<String> SYSTEM_TABLES = Arrays.asList(ERROR_RECORDS_TABLE, BUNDLE_TABLE,
            BUNDLE_DEBUG_INFO_TABLE);

    // TODO: consider whether this should stay permanently
    // it's now necessary for ingestion of perf metrics
    // Limit on how much message the ETL can process. Beyond that the ETL for the bundle fails
    public static final long MAX_RECORDS_PER_BUNDLE_ALLOWED = 1_600_000;

    public static final String VELOCITY_TRANSFORMATION = "VelocityTransformation";

    public static interface MRConfig {
        public static final String HBASE_TABLE_PREFIX_CONFIG_KEY = "ph.analytics.hbase.table-prefix";
        public static final String IMPALA_TABLE_PREFIX_CONFIG_KEY = "ph.analytics.impala.table-prefix";
        public static final String IMPALA_JDBC_URL = "ph.analytics.impala.jdbc-url";
        public static final String IMPALA_JDBC_URL_USER = "ph.analytics.impala.jdbc-url-user";
        public static final String IMPALA_JDBC_URL_PASSWORD = "ph.analytics.impala.jdbc-url-password";
        public static final String IMPALA_RESOURCE_POOL = "ph.analytics.impala.resourcePool";
        public static final String JOB_COLLECTOR_ID_TYPE_CONFIG_KEY = "ph.analytics.job.collectorIdType";
        public static final String ZOOKEEPER_QUORUM = "ph.zookeeper.quorum";
        public static final String HDFS_PARQUET_STAGING_DIRECTORY_KEY = "ph.analytics.hdfsParquetStagingDirectory";
        public static final String SANITIZER_MAX_CELL_SIZE_KEY = "ph.analytics.sanitizer.maxCellSize";
        public static final String METADATA_SCHEMA_NAME = "ph.analytics.metadata.schemaName";
        public static final String CONFIG_DB_URL = "ph.analytics.configDbJdbcUrl";
        public static final String CONFIG_DB_USER = "ph.analytics.configDbJdbcUser";
        public static final String CONFIG_DB_PASSWORD = "ph.analytics.configDbJdbcPassword";
        public static final String GDPR_DB_URL = "ph.analytics.gdprDbJdbcUrl";
        public static final String GDPR_DB_USER = "ph.analytics.gdprDbJdbcUser";
        public static final String GDPR_DB_PASSWORD = "ph.analytics.gdprDbJdbcPassword";
        public static final String GDPR_DB_SCHEMA = "ph.analytics.gdprDbSchema";
        public static final String FAIL_FAST = "ph.analytics.failFast";
        public static final String HISTORY_DATABASE_KEY = "ph.analytics.historyDatabase";

        public static final String PARQUET_SCHEMA_PREFIX = "schema.";
        public static final String PARQUET_SCHEMA_LIST_REGEX = Pattern.quote(PARQUET_SCHEMA_PREFIX) + ".*";

        /**
         * Tables that may receive output during ETL
         *
         * <p>
         * <p>
         */
        public static final String OUTPUT_TABLES = "ph.analytics.outputTables";

        /**
         * Tables which have is_event column in the MDD DB set to true.<br/>
         */
        public static final String EVENT_TABLES = "ph.analytics.eventTables";


        /**
         * Used by ETL determine period of what data is processed and loaded.
         * The date is unix timestamp (milliseconds since epoch), UTC timezone
         *
         */
        public static final String MIN_ARRIVAL_DAY = "ph.analytics.job.minArrivalDay";
        /**
         * @see #MIN_ARRIVAL_DAY
         */
        public static final String MAX_ARRIVAL_DAY = "ph.analytics.job.maxArrivalDay";
        /**
         */
        public static final String CLEAN_BEFORE_LOADING_FLAG = "ph.analytics.job.cleanBeforeLoading";
        /**
         * The option should be specified through command line using:<br>
         * -Dph.analytics.job.cleanBeforeLoading.disableValidation=true
         */
        public static final String CLEAN_BEFORE_LOADING_DISABLE_VALIDATION =
                "ph.analytics.job.cleanBeforeLoading.disableValidation";

    }

}
