package etl;

public interface SparkJobOptions {

    public static final String COLLECTOR_ID_OPT = "--collectorId";
    public static final String ENVIRONMENT_OPT = "--environment";
    public static final String BOOTSTRAPPING_OPT = "--bootstrapping";
    public static final String MAX_BOOTSTRAPED_COLS_OPT = "--maxBootstrappedColumns";
    public static final String PARTITIONS_OPT = "--partitions";
    public static final String OFFSETS_OPT = "--offsets";
    public static final String DURATION_OPT = "--duration";
    public static final String RATE_OPT = "--rate";
    public static final String CLEAN_FIRST_OPT = "--cleanFirst";
    public static final String ARRIVAL_DAY_OPT = "--arrivalDay";
    public static final String FILE_NAME_OPT = "--fileName";
    public static final String KRYO_BUFFER_MAX_OPT = "--kryoBufferMax";
    public static final String META_DATA_SCHEMA_OPT = "--metadataSchema";
    public static final String HISTORY_DB_OPT = "--historyDb";
    public static final String TOPIC_OPT = "--topic";
    public static final String GDPR_CACHE_SIZE_IN_BYTES_OPT = "--gdprCacheSizeInBytes";
    public static final String OFFICIAL_RUN_OPT = "--officialRun";
    public static final String MIGRATE_OPT = "--migrate";
    public static final String ALLOW_DUP_OPT = "--allowDuplicates";
    public static final String KRB_PRINCIPAL = "--krbPrincipal";
    public static final String KRB_KEYTAB = "--krbKeytab";
    public static final String RENEWER_OPT = "--credRenewer";
    public static final String RENEWER_PERIOD_OPT = "--credRenewerPeriodHours";
    public static final String STAGING_HDFS_DIR_OPT = "-PstagingHdfsDir";
}
