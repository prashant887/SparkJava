package hadoop;

import common.ClusterSettings;
import etl.Constants;
import exceptions.SystemException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import etl.Constants.MRConfig;
import java.util.Date;

public class HadoopConfigurationAdapter {

    private static final Logger log = LoggerFactory.getLogger(HadoopConfigurationAdapter.class);

    public static void setupConfiguration(Configuration configuration, ClusterSettings clusterSettings) {
        setRequiredOption(configuration, Constants.MRConfig.IMPALA_JDBC_URL, clusterSettings.getImpalaJdbcUrl());
        setRequiredOption(configuration, Constants.MRConfig.IMPALA_JDBC_URL_USER, clusterSettings.getImpalaJdbcUser());
        setRequiredOption(configuration, Constants.MRConfig.IMPALA_JDBC_URL_PASSWORD, clusterSettings.getImpalaJdbcPassword());

        setOption(configuration, MRConfig.METADATA_SCHEMA_NAME, clusterSettings.getConfigDbMetadataSchema(), null);
        setOption(configuration, MRConfig.CONFIG_DB_URL, clusterSettings.getConfigDbJdbcUrl(), null);
        setOption(configuration, MRConfig.CONFIG_DB_USER, clusterSettings.getConfigDbJdbcUser(), null);
        setOption(configuration, MRConfig.CONFIG_DB_PASSWORD, clusterSettings.getConfigDbJdbcPassword(), null);
        setOption(configuration, MRConfig.GDPR_DB_URL, clusterSettings.getGdprDbJdbcUrl(), null);
        setOption(configuration, MRConfig.GDPR_DB_USER, clusterSettings.getGdprDbJdbcUser(), null);
        setOption(configuration, MRConfig.GDPR_DB_PASSWORD, clusterSettings.getGdprDbJdbcPassword(), null);
        setOption(configuration, MRConfig.GDPR_DB_SCHEMA, clusterSettings.getGdprDbSchema(), null);
        setOption(configuration, MRConfig.ZOOKEEPER_QUORUM, clusterSettings.getZookeeperQuorum(), null);

        setOption(configuration, MRConfig.HISTORY_DATABASE_KEY, clusterSettings.getHistoryDatabase(), null);
    }

    private static void logValue(String key, String value) {
        if (key.contains("assword")) {
            log.debug("Setting configuration option {} = *****", key);
        } else {
            log.debug("Setting configuration option {} = {}", key, value);
        }
    }

    public static void setRequiredOption(Configuration conf, String key, String value) {
        if (value != null) {
            logValue(key, value);
            conf.set(key, value);
        } else {
            throw new SystemException("Missing configuration option " + key
                    + ". The Map Reduce App cannot start unless provided.");
        }
    }

    public static void setOption(Configuration conf, String key, String value, String defaultValue) {
        if (value != null) {
            logValue(key, value);
            conf.set(key, value);
        } else if (defaultValue != null) {
            log.debug("Missing configuration option {}. Setting to default = {}.", key, defaultValue);
            conf.set(key, defaultValue);
        } else {
            log.debug("Missing configuration option {}.", key);
        }
    }

    public static void setOption(Configuration conf, String key, Long value, Long defaultValue) {
        setOption(conf, key, longToString(value), longToString(defaultValue));
    }

    public static void setOption(Configuration conf, String key, boolean value, Boolean defaultValue) {
        setOption(conf, key, booleanToString(value), booleanToString(defaultValue));
    }

    public static void setOption(Configuration conf, String key, Date value, Date defaultValue) {
        Long dateMillis = value != null ? value.getTime() :  null;
        Long defaultDateMillis = defaultValue != null ? defaultValue.getTime() : null;
        setOption(conf, key, dateMillis, defaultDateMillis);
    }

    private static String booleanToString(Boolean value) {
        return value != null ? Boolean.toString(value) : null;
    }

    private static String longToString(Long value) {
        return value != null ? Long.toString(value) : null;
    }

}
