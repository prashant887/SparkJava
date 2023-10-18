package common;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Utility class to help setup configuration for a given environment.
 * <p>
 * Configuration is based on 2 files:
 * <li>cluster.[cluster-name].cfg - there are cluster configuration (physical cluster - e.g HDFS address, kafka address,
 * zookeeper). Currently [cluster-name] can be production or testing. To define your own see below
 * <li>environment.[environment-name].cfg - these are environment (staging/production) configuration (e.g. kafka prefix,
 * kerberos user used, etc.). This is also called "data pipeline". Currently [environment-name] can be staging,
 * production, functest. To define your own see below
 * <br>
 * The idea is to be able to use the same environment configuration against multiple clusters
 * <p>
 * </br>
 * Common cluster settings override order (the later overrides the previous one):
 * <ol>
 * <li> environment settings defined in environment.[environment-name].cfg (the file must be in the classpath)
 * <li> cluster settings defined in cluster.[cluster-name].cfg (the file must be in the classpath)
 * <li> System properties prefixed with <tt>ph.analytics.cluster.</tt>[config-property-name]</br>
 * For example to override a single configuration property you can use system property:
 * <tt>-Dph.analytics.cluster.impalaDbUrl=my-new-db-url</tt>
 * </ol>
 * <p>
 * To override (1.) [environment-name] specify the <tt>-Dph.analytics.cluster.environment=my-environment-name</tt> (and there
 * should be environment.my-environment-name.cfg file on the classpath)
 * </br>
 * To override (2.) [cluster-name] specify the <tt>-Dph.analytics.cluster.cluster=my-cluster-name</tt> (and there should be
 * cluster.my-cluster-name.cfg file on the classpath)
 * </br>
 * Note that the override order is kept the same : system properties still override properties defined in
 * cluster.my-cluster-name.cfg for example.
 * </br></br>
 * <p>
 * If overriding cluster or environment file it has to be on the classpath. To achieve that you can:
 * <li> Either put in commmon project (under /common/src/main/resources/) before compiling and publishing.
 * <li> Or add the file to the classpath at runtime with: <tt>java -Dph.analytics.cluster.cluster=foo -cp
 * "/directory-where-cluster.foo.cfg-is-found:spark-etl-fatjar.jar" ...</tt>
 */
public class ConfigurationProvider {




    public enum Deployment {

        PRODUCTION("production", "production"),

        /**
         * production cluster ({@link ClusterSettings}) settings for staging environment
         */
        STAGING("production", "staging"),

        /**
         * production cluster ({@link ClusterSettings}) settings for dev environment
         * <p> This way a developer can override for its environment.
         * It expects to find file environment.dev.cfg in the classpath.
         */

        CDH6PRODSTG("cdh6prod","cdh6prodstg"),

        DEV("production", "dev");



        private final String cluster;
        private final String environment;


        Deployment(String cluster, String environment) {
            this.cluster = cluster;
            this.environment = environment;
        }

        public String getCluster() {
            return cluster;
        }

        public String getEnvironment() {
            return environment;
        }
    }


    static final String PH_CLUSTER_SETTINGS_PREFIX = "ph.analytics.cluster.";

    private final String cluster;
    private final String environment;

    private final PropertiesLoader propertiesLoader;


    public ConfigurationProvider(String cluster, String environment) {
        this.cluster = cluster;
        this.environment = environment;
        this.propertiesLoader = new PropertiesLoader();
        System.out.println(this.propertiesLoader.load());
    }

    public String getCluster() {
        Properties props = propertiesLoader.load();
        return props.getProperty(PH_CLUSTER_SETTINGS_PREFIX + "cluster", this.cluster);
    }

    public String getEnvironment() {
        Properties props = propertiesLoader.load();
        return props.getProperty(PH_CLUSTER_SETTINGS_PREFIX + "environment", this.environment);
    }

    /**
     * Retrieves the cluster settings for the given environment
     * <p>
     * If there are system properties defined with prefix "ph.analytics.cluster." it will
     * take them as cluster settings. See {@link ClusterSettings} for the expected names of the properties - e.g:
     * for impala jdbc url you should pass system property "ph.analytics.cluster.impalaJdbcUrl"
     */
    public ClusterSettings getClusterSettings() {
        Map<String, String> args = new LinkedHashMap<>();
        args.put("cluster", getCluster());
        args.put("environment", getEnvironment());
        args.putAll(phClusterSystemProperties());
        ClusterSettings settings = new ClusterSettings();
        SettingsParser.parse(SettingsParser.mapToArray(args), settings);
        return settings;
    }

    private Map<String, String> phClusterSystemProperties() {
        Map<String, String> phClusterProperties = new LinkedHashMap<>();
        for (Map.Entry<Object, Object> p : propertiesLoader.load().entrySet()) {
            System.out.println("phClusterSystemProperties : "+p.getKey().toString());
            if (p.getKey().toString().startsWith(PH_CLUSTER_SETTINGS_PREFIX)) {
                String key = (String) p.getKey();
                key = key.substring(PH_CLUSTER_SETTINGS_PREFIX.length());
                phClusterProperties.put(key, (String) p.getValue());
            }
        }
        return phClusterProperties;
    }

    public static ConfigurationProvider create(Deployment env) {
        System.out.println("Developent :"+env.cluster+" "+env.environment);
        return new ConfigurationProvider(env.cluster, env.environment);
    }

    /**
     * This class is extracted out in order to be mocked in unit-tests.
     */
    static class PropertiesLoader {
        public Properties load() {
            return System.getProperties();
        }
    }


    }
