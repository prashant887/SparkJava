package util;

import common.ClusterSettings;
import common.ClusterSettingsBuilder;
import common.ConfigurationProvider;
import config.SparkJobSettings;
import etl.SparkJobOptions;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class SparkJobSettingsParser {


    public static final String COLLECTOR_ID_OPT = SparkJobOptions.COLLECTOR_ID_OPT;
    public static final String ENVIRONMENT_OPT = SparkJobOptions.ENVIRONMENT_OPT;
    public static final String BOOTSTRAPPING_OPT = SparkJobOptions.BOOTSTRAPPING_OPT;
    public static final String MAX_BOOTSTRAPED_COLS_OPT = SparkJobOptions.MAX_BOOTSTRAPED_COLS_OPT;
    public static final String PARTITIONS_OPT = SparkJobOptions.PARTITIONS_OPT;
    public static final String OFFSETS_OPT = SparkJobOptions.OFFSETS_OPT;
    public static final String DURATION_OPT = SparkJobOptions.DURATION_OPT;
    public static final String RATE_OPT = SparkJobOptions.RATE_OPT;
    public static final String CLEAN_FIRST_OPT = SparkJobOptions.CLEAN_FIRST_OPT;
    public static final String ARRIVAL_DAY_OPT = SparkJobOptions.ARRIVAL_DAY_OPT;
    public static final String FILE_NAME_OPT = SparkJobOptions.FILE_NAME_OPT;
    public static final String KRYO_BUFFER_MAX_OPT = SparkJobOptions.KRYO_BUFFER_MAX_OPT;
    public static final String META_DATA_SCHEMA_OPT = SparkJobOptions.META_DATA_SCHEMA_OPT;
    public static final String HISTORY_DB_OPT = SparkJobOptions.HISTORY_DB_OPT;
    public static final String TOPIC_OPT = SparkJobOptions.TOPIC_OPT;
    public static final String GDPR_CACHE_SIZE_IN_BYTES_OPT = SparkJobOptions.GDPR_CACHE_SIZE_IN_BYTES_OPT;
    public static final String OFFICIAL_RUN_OPT = SparkJobOptions.OFFICIAL_RUN_OPT;
    public static final String MIGRATE_OPT = SparkJobOptions.MIGRATE_OPT;
    public static final String ALLOW_DUP_OPT = SparkJobOptions.ALLOW_DUP_OPT;
    public static final String KRB_PRINCIPAL = SparkJobOptions.KRB_PRINCIPAL;
    public static final String KRB_KEYTAB = SparkJobOptions.KRB_KEYTAB;
    public static final String RENEWER_OPT = SparkJobOptions.RENEWER_OPT;
    public static final String RENEWER_PERIOD_OPT = SparkJobOptions.RENEWER_PERIOD_OPT;
    public static final String STAGING_HDFS_DIR_OPT = SparkJobOptions.STAGING_HDFS_DIR_OPT;

    public static final String COLLECTOR_ID_DESCRIPTION = "Collector(s) to start ETL";
    public static final String COLLECTOR_ID_RE_ETL_DESCRIPTION = "Collector to start re-ETL";
    public static final String ENVIRONMENT_DESCRIPTION = "The target deployment. Values are: staging or production or test";
    public static final String BOOTSTRAPPING_DESCRIPTION = "Indicates if enabled or disabled. Values are: true or false";
    public static final String MAX_BOOTSTRAPED_COLS_DESCRIPTION = "Upper limit for number of bootstrapped columns per execution.";
    public static final String PARTITIONS_DESCRIPTION =
            "A list of one or more Kafka partitions in a topic in the form: 0,1,2,.. Only applicable when 1 collector ID is specified.";
    public static final String OFFSETS_DESCRIPTION =
            "A list of offsets for each Kafka partition. The number of offsets must match the number of partitions. in the form: 10,600,3,... Only applicable when 1 collector ID is specified.";
    public static final String DURATION_DESCRIPTION = "Duration of each batch in seconds. If empty, the default will be used. A random value will also be added to mitigate thrashing of the system by ETL jobs.";
    public static final String RATE_DESCRIPTION = "Max rate of messages read per Kafka partition";
    public static final String CLEAN_FIRST_DESCRIPTION =
            "Indicates if the data from the arrival day should be purged or not. Values are: true or false";
    public static final String ARRIVAL_DAY_DESCRIPTION =
            "Arrival day (in seconds) of the data to re-ETL. All files in the directory will be re-ETLed.";
    public static final String FILE_NAME_DESCRIPTION =
            "The full file path of the sequence file to re-ETL. Used to individually re-ETL a file. The arrival day option is ignored.";
    public static final String KRYO_BUFFER_MAX_DESCRIPTION = "Maximum allowable size of Kryo serialization buffer";
    public static final String META_DATA_SCHEMA_DESCRIPTION = "Schema for collector ID";
    public static final String HISTORY_DB_DESCRIPTION = "The history DB. ETL will write to this DB.";
    public static final String TOPIC_DESCRIPTION = "The Kafka topic to read data from.";
    public static final String GDPR_CACHE_SIZE_IN_BYTES_DESCRIPTION = "Size of GDPR cache size in bytes";
    public static final String OFFICIAL_RUN_DESCRIPTION =
            "Indiciates if this is an offical run. Official runs will have their parquet files loaded from a known location at startup";
    public static final String MIGRATE_DESCRIPTION = "ETL will be running in migrate mode.";
    public static final String ALLOW_DUP_DESCRIPTION = "Allow multiple ETL jobs to run for the same collector and history DB";
    public static final String KRB_PRINCIPAl_DESCRIPTION = "Kerberos principal user name to use to connect to hdfs";
    public static final String KRB_KEYTAB_DESCRIPTION = "Path to keytab file of the kerberoes principal";
    public static final String RENEWER_DESCRIPTION = "Enable periodic credential renewals. Only Hadoop Delegation tokens are supported.";
    public static final String RENEWER_PERIOD_DESCRIPTION = "Min hours until credentials are renewed";
    public static final String STAGING_HDFS_DIR_DESCRIPTION = "Staging directory where ETL will temporarily place files when executor initially writes them and when driver is coalesing all files to load to DB. If writing to an encyrpted DB, this stating directory should be inside the same encrypted zone.";

    public static final int DEFAULT_SPARK_DURATION_SEC = 1200;
    public static final int MAX_RANDOM_DUR_SEC = 30; // Used to randomize the default duration
    public static final int DEFAULT_SPARK_MAX_RATE_PER_PARTITION = 10;
    public static final String DEFAULT_KRYO_BUFFER_MAX = "64m";
    public static final int DEFAULT_GDRP_CACHE_SIZE_IN_BYTES = 100000000; // 100 MB upper bound
    public static final int DEFAULT_RENEWER_PERIOD_HOURS = 12;


    private static class StreamingSettings {
        public String collectorId="collector_1,collector_2";

        // This is really a deployment (as defined in *.common.ConfigurationProvider.Deployment)
        // So TODO: consider renaming it.
        public String environment="DEV";

        public boolean bootstrapping = false;

        public int maxBootstrappedColumns = 1000;

        public String metadataSchema;

        public String historyDb;

        public String partitions;

        public String offsets;

        public int duration = DEFAULT_SPARK_DURATION_SEC + ThreadLocalRandom.current().nextInt(1, MAX_RANDOM_DUR_SEC);

        public int rate = DEFAULT_SPARK_MAX_RATE_PER_PARTITION;

        public String kryoBufferMax = DEFAULT_KRYO_BUFFER_MAX;

        public String topic = "";

        public int gdprCacheSizeInBytes = DEFAULT_GDRP_CACHE_SIZE_IN_BYTES;

        public boolean officialRun = false;

        public boolean migrate = false;

        public boolean allowDuplicates = false;

        public String krbPrincipal;

        public String krbKeytab;

        public boolean renewerEnabled = false;


        public int renewerPeriodHours = DEFAULT_RENEWER_PERIOD_HOURS;

        public String stagingHdfsDir = null;
    }

    private static class ReEtlSettings {
        public String collectorId;

        public String environment;

        public String metadataSchema;

        public String historyDb;

        public boolean bootstrapping = false;

        public int maxBootstrappedColumns = 1000;

        public boolean cleanFirst = false;

        public long arrivalDay = 0;

        public String fileName;

        public String kryoBufferMax = DEFAULT_KRYO_BUFFER_MAX;

        public String krbPrincipal;

        public String krbKeytab;
    }

   private ClusterSettings clusterSettings;
    private final SparkJobSettings.SparkPurpose sparkPurpose;
    private Map<String, String> topics;
    private String environment;
    private boolean bootstrapping = false;
    private int maxBootstrappedColumns = 1000;
    private boolean officalRun = false;
    private boolean migrate = false;
    private boolean allowDuplicates = false;

    private String partitions;
    private String offsets;
    private int duration;
    private int rate;
    private PartitionOffsetRetriever partitionOffsetRetriever;
    private String kryoBufferMax;
    private int gdprCacheSizeInBytes;

    private boolean cleanFirst;
    private long arrivalDay;
    private String fileName;

    private boolean renewerEnabled;
    private int renewerPeriodHours;
    private String stagingHdfsDir;

    public SparkJobSettingsParser(SparkJobSettings.SparkPurpose sparkPurpose, String[] args) throws Exception {
        this.sparkPurpose = sparkPurpose;
        if (sparkPurpose == SparkJobSettings.SparkPurpose.STREAMING) {
            System.out.println("STREAMING");
            StreamingSettings settings = new StreamingSettings();
           // parse(MddSparkEtlApp.class.getSimpleName(), args, settings);
            setCommonParams(settings.collectorId, settings.environment, settings.bootstrapping, settings.maxBootstrappedColumns, settings.topic);
            overrideClusterSettings(settings.metadataSchema, settings.historyDb, settings.krbPrincipal, settings.krbKeytab);


        }
        else {
            System.out.println("ETL");

            ReEtlSettings settings = new ReEtlSettings();

        }
    }

    public ClusterSettings getClusterSettings() {
        System.out.println(clusterSettings);
        return clusterSettings;
    }

    private void setCommonParams(String rawCollectorIdString, String env, boolean bootstrapping, int maxBootstrappedColumns, String topic)
            throws Exception {
        this.environment = env;
        System.out.println(" environment :"+this.environment);

        // Need to set this first because it contains topic prefix
        this.clusterSettings = ConfigurationProvider.create(ConfigurationProvider.Deployment.valueOf(env.toUpperCase())).getClusterSettings();
        System.out.println(" setCommonParams :"+this.clusterSettings);
        // String may contain more than one collector
        String[] collectorIds = rawCollectorIdString.split(",");
        Map<String, String> topics = new HashMap<>();

        if (topic.isEmpty()) {
            for (String collectorId : collectorIds) {
                topics.put(collectorId, getTopic(collectorId));
            }
        } else if (collectorIds.length == 1) {
            topics.put(collectorIds[0], topic);
        } else {
            String msg = "Topic override option only valid for 1 collector! Number of collectors: " + collectorIds.length;
            throw new IllegalArgumentException(msg);
        }

        this.topics = topics;
        this.bootstrapping = bootstrapping;
        this.maxBootstrappedColumns = maxBootstrappedColumns;
        nullHistoryDbFromClusterSettings();
    }

    // The datalake is initially loaded from the common config file. The datalake value
    // needs to either come from the MDD or passed in as an argument.
    private void nullHistoryDbFromClusterSettings() throws Exception {
        ClusterSettingsBuilder newSettingsBuilder = new ClusterSettingsBuilder(clusterSettings);
        newSettingsBuilder.setHistoryDatabase(null);
        clusterSettings = newSettingsBuilder.build();
    }

    private String getTopic(String collectorId) {
        return clusterSettings.getKafkaTopicPrefix() + collectorId;
    }



    private void overrideClusterSettings(String metaDataSchema, String historyDb, String krbPrincipal, String krbKeytab) throws Exception {
        ClusterSettingsBuilder newSettingsBuilder = new ClusterSettingsBuilder(clusterSettings);
        if (metaDataSchema != null) {
            newSettingsBuilder.setConfigDbMetadataSchema(metaDataSchema);
        }
        if (historyDb != null) {
            historyDb = historyDb.toLowerCase();
            newSettingsBuilder.setHistoryDatabase(historyDb);
        }
        if (krbPrincipal != null) {
            newSettingsBuilder.setKrbPrincipal(krbPrincipal);
        }
        if (krbKeytab != null) {
            newSettingsBuilder.setKrbKeytab(krbKeytab);
        }
        clusterSettings = newSettingsBuilder.build();
    }
}
