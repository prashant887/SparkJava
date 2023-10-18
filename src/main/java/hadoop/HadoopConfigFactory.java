package hadoop;

import common.ClusterSettings;
import exceptions.FatalConfigurationException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;
import java.net.URI;

/**
 * A factory class for creating Configuration instances
 *
 * <p>
 * Any component that wants to talk to HDFS or YARN should use {@link #create()} unless it can obtain configuration from
 * its container e.g. Map/Reduce or Spark context. ClusterSettings intentionally doesn't provide configuration like HDFS
 * address or YARN resource manager address. Unlike a fixed address, configuration obtained here would work correctly
 * with YARN HA and HDFS HA.
 *
 * @see org.apache.hadoop.conf.Configuration
 */
public final class HadoopConfigFactory {

    private HadoopConfigFactory() { }

    public static void login(Configuration configuration, String krbPrincipal, String krbKeytabPath) {
        UserGroupInformation.setConfiguration(configuration);
        try {
            UserGroupInformation.loginUserFromKeytab(krbPrincipal, krbKeytabPath);
            System.out.println("Kerberos authentication was successful.");
        } catch (IOException e) {
            System.out.println("Failed to login with principal " + krbPrincipal + " and keytab file '" + krbKeytabPath
                    + "'. Reason: " + ExceptionUtils.getRootCauseMessage(e));
        }
    }

    /**
     * Creates a HDFS and YARN configuration based on native Hadoop configuration on the classpath.
     *
     * <p>
     * When running in production, the wrapper script or container should include Hadoop configuration on the classpath.
     * When running locally, see projects/cluster-dev-config/README.md
     *
     * @return Hadoop Configuration object, not null
     * @throws FatalConfigurationException
     *            if HADOOP_CONF_DIR environment var doesn't point to a directory
     */
    public static Configuration create() {
        // The following two lines ensure that the static constructors of HdfsConfiguration and YarnConfiguration
        // have been executed. See the javadoc of HdfsConfiguration.init() for details.
        HdfsConfiguration.init();
        Configuration conf = new YarnConfiguration();

        final URI defaultUri = FileSystem.getDefaultUri(conf);
        if (!"hdfs".equalsIgnoreCase(defaultUri.getScheme())) {
            throw new FatalConfigurationException(String.format("Incomplete Hadoop configuration: %s.\n"
                    + "      Most likely there are no *-site.xml files in the classpath.\n"
                    + "      In production setup you need make sure to add the hadoop conf directory"
                    + " (usually /etc/hadoop/conf or /etc/hive/conf) to classpath.\n"
                    + "      In local setup make sure to execute build.[sh|bat] get-cluster-configuration"
                    + " and then add the downloaded *-site.xml files to classpath.\n"
                    + "      For tests use HadoopTestConfig.registerTestConfig instead which adds them automatically.\n", conf));
        }

      /* we don't use or need parquet metadata summary (_metadata file created after M/R job).
         and it won't work if two entities have the same attribute/column but with different types */
        conf.setBoolean("parquet.enable.summary-metadata", false);

        // TODO Move to HCatalogFactory ?
        conf.setBoolean("hive.metastore.sasl.enabled", true);
        conf.set("hive.metastore.kerberos.principal", "hive/_HOST@PHONEHOME.VMWARE.COM");

        // TODO Rely on yarn-site.xml if it's there.
        conf.setBoolean("mapreduce.reduce.speculative", true);
        return conf;
    }

    /** Combines {@link #create()} and the login method
     *
     * @param clusterSettings source of authentication information
     * @return Hadoop Configuration object, not null
     */
    public static Configuration createAuthenticated(ClusterSettings clusterSettings) {
        Configuration conf = create();
        login(conf, clusterSettings.getKrbPrincipal(), clusterSettings.getKrbKeytab());
        return conf;
    }
}
