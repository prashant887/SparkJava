package hcat;

import common.HCatException;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * HCatalog interface implementation through Java Hcatalog API.
 * <p>
 * If connecting to remote metastore "hive.metastore.uri" must be set appropriately on the hadoop configuration.<br>
 * The configuration is set by default from the "hive-site.xml" file in the default Hadoop configuration directory (most
 * probably /etc/hive/conf.cloudera.hive/). <br>
 * The configuration however could be overridden by directly setting "hive.metastore.uris" property on the configuration
 * object itself from source code like:
 *
 * <pre>
 * {@code conf.set("hive.metastore.uris", "thrift://ph-hdp-prd-cm.phonehome.vmware.com:9083"); }
 * </pre>
 *
 * Actually the initial implementation of VAC source was using that approach until metastore HA was implemented.<br>
 * Note: the "hive.metastore.uris" accepts a list of comma separated URIs, so one can list all metastore host.
 * <p>
 * If connecting to kerberos secured cluster:<br>
 *
 * <pre>
 * {@code conf.setBoolean("hive.metastore.sasl.enabled", true) }
 * {@code conf.set("hive.metastore.kerberos.principal", "hive/_HIVESERVER_HOST@YOUR-REALM.COM") }
 * </pre>
 *
 * Also the application needs to be authenticated or logged in. See
 * {@link org.apache.hadoop.security.UserGroupInformation} or
 */
public class HCatalogImpl implements HCatalogInterface{


    public HCatalogImpl(Configuration hcatConf, HCatalogFactory.ChangeSynchronization globalSchemaSync) {
    }

    @Override
    public UpdateResult updateTableSchema(TableSchema tableSchema) throws HCatalogException {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
