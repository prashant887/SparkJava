package common;

import org.apache.hadoop.shaded.org.apache.commons.beanutils.BeanUtils;

public class ClusterSettingsBuilder {

    private String remoteHost;
    private String impalaJdbcUrl;
    private String impalaJdbcUser;
    private String impalaJdbcPassword;
    private String zookeeperQuorum;
    private String configDbJdbcUrl;
    private String gdprDbJdbcUrl;
    private String gdprDbJdbcUser;
    private String gdprDbJdbcPassword;
    private String configDbJdbcUser;
    private String configDbJdbcPassword;
    private String configDbPhSystemSchema;
    private String configDbMetadataSchema;
    private String gdprDbSchema;
    private String historyDatabase;
    private String krbPrincipal;
    private String krbKeytab;
    private String kafkaBrokerList;
    private String kafkaTopicPrefix;
    private String flumeBackupHdfsPath;
    private String configDbStreamingSchema;
    private String rtsUrl;
    private String nonCdfRtsUrl;
    private String telemetryRtsUrl;
    private String restDbUrl;
    private String influxDbHost;
    private String graphiteMetricsHost;
    private String streamingEtlStagingHdfsDir;
    private String reEtlStagingHdfsDir;
    private String etlJdbcUrl;
    private String etlJdbcUser;
    private String etlJdbcPassword;

    /**
     * Initializes a builder using an existing, possibly empty, settings as template.
     *
     * @param template
     */
    public ClusterSettingsBuilder(ClusterSettings template) throws Exception {
        try {
            BeanUtils.copyProperties(this, template);
        } catch (ReflectiveOperationException e) {
            throw new Exception("ReflectiveOperationException in hardcoded invokation.", e);
        }
    }

    public ClusterSettings build() {
        return new ClusterSettings(remoteHost, impalaJdbcUrl, impalaJdbcUser, impalaJdbcPassword, zookeeperQuorum,
                configDbJdbcUrl, gdprDbJdbcUrl, gdprDbJdbcUser, gdprDbJdbcPassword, configDbJdbcUser, configDbJdbcPassword, configDbPhSystemSchema,
                configDbMetadataSchema, gdprDbSchema, historyDatabase, krbPrincipal, krbKeytab, kafkaBrokerList,
                kafkaTopicPrefix, flumeBackupHdfsPath, configDbStreamingSchema, rtsUrl, restDbUrl, nonCdfRtsUrl,
                telemetryRtsUrl, influxDbHost, graphiteMetricsHost, streamingEtlStagingHdfsDir, reEtlStagingHdfsDir, etlJdbcUrl, etlJdbcUser, etlJdbcPassword);
    }

    public void setRemoteHost(String remoteHost) {
        this.remoteHost = remoteHost;
    }

    public void setImpalaJdbcUrl(String impalaJdbcUrl) {
        this.impalaJdbcUrl = impalaJdbcUrl;
    }

    public void setImpalaJdbcUser(String impalaJdbcUser) {
        this.impalaJdbcUser = impalaJdbcUser;
    }

    public void setImpalaJdbcPassword(String impalaJdbcPassword) {
        this.impalaJdbcPassword = impalaJdbcPassword;
    }

    public void setZookeeperQuorum(String zookeeperQuorum) {
        this.zookeeperQuorum = zookeeperQuorum;
    }

    public void setConfigDbJdbcUrl(String configDbJdbcUrl) {
        this.configDbJdbcUrl = configDbJdbcUrl;
    }

    public void setGdprDbJdbcUrl(String gdprDbJdbcUrl) {
        this.gdprDbJdbcUrl = gdprDbJdbcUrl;
    }

    public void setGdprDbJdbcUser(String gdprDbJdbcUser) {
        this.gdprDbJdbcUser = gdprDbJdbcUser;
    }

    public void setGdprDbJdbcPassword(String gdprDbJdbcPassword) {
        this.gdprDbJdbcPassword = gdprDbJdbcPassword;
    }

    public void setConfigDbJdbcUser(String configDbJdbcUser) {
        this.configDbJdbcUser = configDbJdbcUser;
    }

    public void setConfigDbJdbcPassword(String configDbJdbcPassword) {
        this.configDbJdbcPassword = configDbJdbcPassword;
    }

    public void setConfigDbPhSystemSchema(String configDbPhSystemSchema) {
        this.configDbPhSystemSchema = configDbPhSystemSchema;
    }

    public void setConfigDbMetadataSchema(String configDbMetadataSchema) {
        this.configDbMetadataSchema = configDbMetadataSchema;
    }

    public void setGdprDbSchema(String gdprDbSchema) {
        this.gdprDbSchema = gdprDbSchema;
    }

    public void setHistoryDatabase(String historyDatabase) {
        this.historyDatabase = historyDatabase;
    }

    public void setKrbPrincipal(String krbPrincipal) {
        this.krbPrincipal = krbPrincipal;
    }

    public void setKrbKeytab(String krbKeytab) {
        this.krbKeytab = krbKeytab;
    }

    public void setKafkaBrokerList(String kafkaBrokerList) {
        this.kafkaBrokerList = kafkaBrokerList;
    }

    public void setKafkaTopicPrefix(String kafkaTopicPrefix) {
        this.kafkaTopicPrefix = kafkaTopicPrefix;
    }

    public void setFlumeBackupHdfsPath(String flumeBackupHdfsPath) {
        this.flumeBackupHdfsPath = flumeBackupHdfsPath;
    }

    public void setConfigDbStreamingSchema(String configDbStreamingSchema) {
        this.configDbStreamingSchema = configDbStreamingSchema;
    }

    public void setRtsUrl(String rtsUrl) {
        this.rtsUrl = rtsUrl;
    }

    public void setNonCdfRtsUrl(String nonCdfRtsUrl) {
        this.nonCdfRtsUrl = nonCdfRtsUrl;
    }

    public void setTelemetryRtsUrl(String telemetryRtsUrl) {
        this.telemetryRtsUrl = telemetryRtsUrl;
    }

    public void setRestDbUrl(String restDbUrl) {
        this.restDbUrl = restDbUrl;
    }

    public void setInfluxDbHost(String influxDbHost) {
        this.influxDbHost = influxDbHost;
    }

    public void setGraphiteMetricsHost(String graphiteMetricsHost) {
        this.graphiteMetricsHost = graphiteMetricsHost;
    }

    public void setStreamingEtlStagingHdfsDir(String streamingEtlStagingHdfsDir) {
        this.streamingEtlStagingHdfsDir = streamingEtlStagingHdfsDir;
    }

    public void setReEtlStagingHdfsDir(String reEtlStagingHdfsDir) {
        this.reEtlStagingHdfsDir = reEtlStagingHdfsDir;
    }

    public void setEtlJdbcUrl(String etlJdbcUrl) {
        this.etlJdbcUrl = etlJdbcUrl;
    }

    public void setEtlJdbcUser(String etlJdbcUser) {
        this.etlJdbcUser = etlJdbcUser;
    }

    public void setEtlJdbcPassword(String etlJdbcPassword) {
        this.etlJdbcPassword = etlJdbcPassword;
    }

}
