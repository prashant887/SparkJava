package hcat;

import common.ClusterSettings;
import hadoop.HadoopConfigFactory;
import hadoop.HadoopConfigurationAdapter;
import hadoop.HadoopFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;

public class HCatalogFactory implements HadoopFactory<HCatalogInterface, HCatalogException> {

    private final ChangeSynchronization globalSchemaSync;

    /**
     * The strategy for making changes visible to other schema users. For example, schema users are Impala JDBC
     * connections and other instances of {@link HCatalogInterface} produced by this factory
     */
    public enum ChangeSynchronization {

        /**
         * All schema changes are visible to other schema users after a schema operation completes.
         */
        GLOBAL,

        /**
         * The schema changes are guaranteed to be visible only to the same {@link HCatalogInterface} instance. Some
         * changes may be visible to other callers as well. Changes are executed much faster.
         */
        LOCAL
    }

    public HCatalogFactory(ChangeSynchronization globalSchemaSync) {
        this.globalSchemaSync = globalSchemaSync;
    }


    public HCatalogInterface create(ClusterSettings cluster) throws HCatalogException {
        System.out.println("Setup and create Hcatalog Interface...");

        Configuration conf = getHcatConfiguration(cluster);

        return create(conf);
    }

    @Override
    public HCatalogInterface create(Configuration conf) throws HCatalogException {

        Configuration hcatConf = getHcatConfiguration(conf);
        return new HCatalogImpl(hcatConf, globalSchemaSync);
    }

    /**
     * Creates Hadoop Configuration based on passed ClusterSettings that allows one to connect to Hive service.
     * It handles authentication to Kerberos as well.
     */
    public static Configuration getHcatConfiguration(ClusterSettings cluster) {
        Configuration conf = HadoopConfigFactory.createAuthenticated(cluster);

        System.out.println("Set metastore and impala configuration used by HcatalogInterface");
        HadoopConfigurationAdapter.setupConfiguration(conf, cluster);
        return conf;
    }

    private static Configuration getHcatConfiguration(Configuration conf) {
        Configuration hcatConf = new HiveConf(conf, HCatalogFactory.class);

        if (UserGroupInformation.isSecurityEnabled()) {
            System.out.println("Security is enabled. Set metastore security configuration.");
            // needs to be set after creating HiveConf or it won't get pick up.
            setIfNotNull(hcatConf, HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname,
                    conf.get(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname));
            setIfNotNull(hcatConf, HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname,
                    conf.get(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname));
        }
        return hcatConf;
    }

    private static void setIfNotNull(Configuration conf, String name, String value) {
        if (value != null) {
            conf.set(name, value);
        }
    }
}