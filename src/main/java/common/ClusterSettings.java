package common;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import com.beust.jcommander.JCommander;
import java.io.Serializable;

public class ClusterSettings implements Serializable {

    private static final long serialVersionUID = -2784323168017462872L;

    private static final String DESCRIPTION_REMOTE_HOST =
            "The remote host on the hadoop cluster where the ETL job is going to be started.";
    private static final String DESCRIPTION_IMPALA_JDBC_URL = "JDBC URL for Impala";
    private static final String DESCRIPTION_IMPALA_JDBC_USER = "User name used to authenticate to Impala with JDBC";
    private static final String DESCRIPTION_IMPALA_JDBC_PASSWORD =
            "User password used to authentiate to Impala with JDBC";
    private static final String DESCRIPTION_ZOOKEEPER_QUORUM = "Zookeeper Quorum servers addresses.";
    private static final String DESCRIPTION_CONFIG_DB_JDBC_URL =
            "The configuration database URL(connection string). Expected to be jdbc URL.";
    private static final String DESCRIPTION_GDPR_DB_JDBC_URL =
            "The GDPR database URL(connection string). Expected to be jdbc URL.";
    private static final String DESCRIPTION_GDPR_DB_JDBC_USER =
            "The GDPR database user to connect as";
    private static final String DESCRIPTION_GDPR_DB_JDBC_PASSWORD =
            "The GDPR database user's password";
    private static final String DESCRIPTION_CONFIG_DB_JDBC_USER = "The configuration database user to connect as";
    private static final String DESCRIPTION_CONFIG_DB_JDBC_PASSWORD = "The configuration database user's password";
    private static final String DESCRIPTION_CONFIG_DB_PH_SYSTEM_SCHEMA =
            "The Phonehome system schema where the phonehome analytics system tables reside in.";
    private static final String DESCRIPTION_CONFIG_DB_METADATA_SCHEMA =
            "The metadata schema of the metadata repository to which to connect.";
    private static final String DESCRIPTION_GDPR_DB_SCHEMA =
            "The schema of the GDPR repository to which to connect.";
    private static final String DESCRIPTION_HISTORY_DATABASE =
            "The name of the history database in impala/hive which contains the data. "
                    + "Usually history for production and stg__history for staging.";
    private static final String DESCRIPTION_KERBEROS_PRINCIPAL =
            "Kerberos principal user name to use to connect to hdfs";
    private static final String DESCRIPTION_KERBEROS_KEYTAB = "Path to keytab file of the kerberoes principal";
    private static final String DESCRIPTION_KAFKA_BROKER_LIST = "List of Kafka brokers";
    private static final String DESCRIPTION_KAFKA_TOPIC_PREFIX = "The prefix of Kafka topics";
    private static final String DESCRIPTION_FLUME_BACKUP_HDFS_PATH = "HDFS URL and directory where Flume backup writes to";
    private static final String DESCRIPTION_CONFIG_DB_STREAMING_SCHEMA =
            "The schema where streaming related tables reside in.";
    private static final String DESCRIPTION_RTS_URL =
            "URL of the primary RTS for the given data pipeline(environment).";
    private static final String DESCRIPTION_NONCDF_RTS_URL =
            "URL of the primary non CDF data RTS for the given data pipeline(environment)."
                    + "CDF type of data must use rtsUrl endpoint instead.";
    private static final String DESCRIPTION_TELEMETRY_RTS_URL =
            "URL of the primary RTS on the cluster, used for telemetry."
                    + "The differnece between rts url and telemtry rts url is to be able to send telemetry"
                    + " for all data pipelines (from staging ETL's own telemetry should go to production)";
    private static final String DESCRIPTION_REST_DB_URL =
            "URL of the VAC REST DB, formerly known as the Collection Platform Portal/Django. The URL doesn't include /ph/v1/api which is expected by DalClientBuilder";
    private static final String DESCRIPTION_INFLUX_DB_HOST =
            "Host of the InfluxDB, this should be used by monitoring tools to send metrics.";
    private static final String DESCRIPTION_GRAPHITE_METRICS_HOST =
            "Host which accepts graphite metrics, this should be used only for graphite metrics.";
    private static final String DESCRIPTION_STREAMING_ETL_STAGING_HDFS_DIR = "Staging directory on HDFS for streaming ETL";
    private static final String DESCRIPTION_RE_ETL_STAGING_HDFS_DIR = "Staging directory on HDFS for streaming re-ETL";
    private static final String DESCRIPTION_ETL_JDBC_URL = "JDBC URL for ETL";
    private static final String DESCRIPTION_ETL_JDBC_USER = "User name used to authenticate with JDBC for ETL";
    private static final String DESCRIPTION_ETL_JDBC_PASSWORD =
            "User password used to authentiate with JDBC for ETL";

    private final String remoteHost;
    private final String impalaJdbcUrl;
    private final String impalaJdbcUser;
    private final String impalaJdbcPassword;
    private final String zookeeperQuorum;
    private final String configDbJdbcUrl;
    private final String gdprDbJdbcUrl;
    private final String gdprDbJdbcUser;
    private final String gdprDbJdbcPassword;
    private final String configDbJdbcUser;
    private final String configDbJdbcPassword;
    private final String configDbPhSystemSchema;
    private final String configDbMetadataSchema;
    private final String gdprDbSchema;
    @Parameter(names = { "--historyDatabase" }, description = DESCRIPTION_HISTORY_DATABASE)

    private final String historyDatabase;
    private final String krbPrincipal;
    private final String krbKeytab;

    @Parameter(names = { "--kafkaBrokerList" }, description = DESCRIPTION_KAFKA_BROKER_LIST)
    private final String kafkaBrokerList;
    private final String kafkaTopicPrefix;
    private final String flumeBackupHdfsPath;
    private final String configDbStreamingSchema;
    private final String rtsUrl;
    private final String nonCdfRtsUrl;
    private final String telemetryRtsUrl;
    private final String restDbUrl;
    private final String influxDbHost;
    private final String graphiteMetricsHost;
    private final String streamingEtlStagingHdfsDir;
    private final String reEtlStagingHdfsDir;
    private final String etlJdbcUrl;
    private final String etlJdbcUser;
    private final String etlJdbcPassword;

    public ClusterSettings() {
        this(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
    }

    public ClusterSettings(String remoteHost, String impalaJdbcUrl, String impalaJdbcUser, String impalaJdbcPassword,
                           String zookeeperQuorum, String configDbJdbcUrl, String gdprDbJdbcUrl, String gdprDbJdbcUser, String gdprDbJdbcPassword, String configDbJdbcUser,
                           String configDbJdbcPassword, String configDbPhSystemSchema, String configDbMetadataSchema,
                           String gdprDbSchema,
                           String historyDatabase, String krbPrincipal, String krbKeytab,
                           String kafkaBrokerList, String kafkaTopicPrefix, String flumeBackupHdfsDir, String configDbStreamingSchema,
                           String rtsUrl, String restDbUrl, String nonCdfRtsUrl, String telemetryRtsUrl, String influxDbHost, String graphiteMetricsHost,
                           String streamingEtlStagingHdfsDir, String reEtlStagingHdfsDir,
                           String etlJdbcUrl, String etlJdbcUser, String etlJdbcPassword) {
        this.remoteHost = remoteHost;
        this.impalaJdbcUrl = impalaJdbcUrl;
        this.impalaJdbcUser = impalaJdbcUser;
        this.impalaJdbcPassword = impalaJdbcPassword;
        this.zookeeperQuorum = zookeeperQuorum;
        this.configDbJdbcUrl = configDbJdbcUrl;
        this.gdprDbJdbcUrl = gdprDbJdbcUrl;
        this.gdprDbJdbcUser = gdprDbJdbcUser;
        this.gdprDbJdbcPassword = gdprDbJdbcPassword;
        this.configDbJdbcUser = configDbJdbcUser;
        this.configDbJdbcPassword = configDbJdbcPassword;
        this.configDbPhSystemSchema = configDbPhSystemSchema;
        this.configDbMetadataSchema = configDbMetadataSchema;
        this.gdprDbSchema = gdprDbSchema;
        this.historyDatabase = historyDatabase;
        this.krbPrincipal = krbPrincipal;
        this.krbKeytab = krbKeytab;
        this.kafkaBrokerList = kafkaBrokerList;
        this.kafkaTopicPrefix = kafkaTopicPrefix;
        this.flumeBackupHdfsPath = flumeBackupHdfsDir;
        this.configDbStreamingSchema = configDbStreamingSchema;
        this.rtsUrl = rtsUrl;
        this.restDbUrl = restDbUrl;
        this.nonCdfRtsUrl = nonCdfRtsUrl;
        this.telemetryRtsUrl = telemetryRtsUrl;
        this.influxDbHost = influxDbHost;
        this.graphiteMetricsHost = graphiteMetricsHost;
        this.streamingEtlStagingHdfsDir = streamingEtlStagingHdfsDir;
        this.reEtlStagingHdfsDir = reEtlStagingHdfsDir;
        this.etlJdbcUrl = etlJdbcUrl;
        this.etlJdbcUser = etlJdbcUser;
        this.etlJdbcPassword = etlJdbcPassword;
    }

    public String getRemoteHost() {
        return remoteHost;
    }

    public String getImpalaJdbcUrl() {
        return impalaJdbcUrl;
    }

    public String getImpalaJdbcUser() {
        return this.impalaJdbcUser;
    }

    public String getImpalaJdbcPassword() {
        return this.impalaJdbcPassword;
    }

    public String getZookeeperQuorum() {
        return zookeeperQuorum;
    }

    public String getConfigDbJdbcUrl() {
        return configDbJdbcUrl;
    }

    public String getGdprDbJdbcUrl() {
        return gdprDbJdbcUrl;
    }

    public String getGdprDbJdbcUser() {
        return gdprDbJdbcUser;
    }

    public String getGdprDbJdbcPassword() {
        return gdprDbJdbcPassword;
    }

    public String getConfigDbJdbcUser() {
        return configDbJdbcUser;
    }

    public String getConfigDbJdbcPassword() {
        return configDbJdbcPassword;
    }

    public String getConfigDbPhSystemSchema() {
        return configDbPhSystemSchema;
    }

    public String getConfigDbMetadataSchema() {
        return configDbMetadataSchema;
    }

    public String getGdprDbSchema() {
        return gdprDbSchema;
    }

    public String getHistoryDatabase() {
        return historyDatabase == null ? null : historyDatabase.toLowerCase();
    }

    public String getKrbPrincipal() {
        return krbPrincipal;
    }

    public String getKrbKeytab() {
        return krbKeytab;
    }

    public JdbcEndpoint getConfigDbJdbcEndpoint() {
        return new JdbcEndpoint(configDbJdbcUrl, configDbJdbcUser, configDbJdbcPassword);
    }

    public JdbcEndpoint getGdprDbJdbcEndpoint() {
        return new JdbcEndpoint(gdprDbJdbcUrl, gdprDbJdbcUser, gdprDbJdbcPassword);
    }

    public JdbcEndpoint getImpalaJdbcEndpoint() {
        return new JdbcEndpoint(impalaJdbcUrl, impalaJdbcUser, impalaJdbcPassword);
    }

    public JdbcEndpoint getEtlJdbcEndpoint() {
        return new JdbcEndpoint(etlJdbcUrl, etlJdbcUser, etlJdbcPassword);
    }

    public String getKafkaBrokerList() {
        return this.kafkaBrokerList;
    }

    public String getKafkaTopicPrefix() {
        return this.kafkaTopicPrefix;
    }

    public String getFlumeBackupHdfsPath() {
        return this.flumeBackupHdfsPath;
    }

    public String getConfigDbStreamingSchema() {
        return this.configDbStreamingSchema;
    }

    public String getRtsUrl() {
        return this.rtsUrl;
    }

    public String getNonCdfRtsUrl() {
        return this.nonCdfRtsUrl;
    }

    public String getTelemetryRtsUrl() {
        return this.telemetryRtsUrl;
    }

    public String getRestDbUrl() {
        return restDbUrl;
    }

    public String getInfluxDbHost() {
        return this.influxDbHost;
    }

    public String getGraphiteMetricsHost() {
        return this.graphiteMetricsHost;
    }

    public String getStreamingEtlStagingHdfsDir() {
        return this.streamingEtlStagingHdfsDir;
    }

    public String getReEtlStagingHdfsDir() {
        return this.reEtlStagingHdfsDir;
    }

    public String getEtlJdbcUrl() {
        return this.etlJdbcUrl;
    }

    public String getEtlJdbcUser() {
        return this.etlJdbcUser;
    }

    public String getEtlJdbcPassword() {
        return this.etlJdbcPassword;
    }

    @Override
    public String toString() {
        String tmp = ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        return (tmp+",").replaceAll("assword=[^,]*[,]", "assword=*****,");
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

}
