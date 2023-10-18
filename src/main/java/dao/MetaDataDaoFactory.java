package dao;

import common.ClusterSettings;
import common.JdbcEndpoint;
import org.hibernate.SessionFactory;
import util.JdbcUrlUtils;

/**
 * A factory for creating MetaDataDao instances. The created by this factory DAO objects will revert the transaction in
 * case of exception. The transaction boundary and management should be defined by the users themselves. This is done in
 * order to provide bigger flexibility and to allow the users to define transactions spanning several methods and DB
 * operations. <br/>
 * Sample usage:
 *
 * <pre>
 * try (MetaDataDaoFactory metaDataDaoFactory = new MetaDataDaoFactory(schema)) {
 *    MetaDataDao mdd = metaDataDaoFactory.create();
 *    mdd.openSessionWithTransaction();
 *    // do something with mdd e.g. mdd.getSourceInfosByCollector(collector);
 *    mdd.commitTransactionAndCloseSession();
 * }
 * </pre>
 */
public class MetaDataDaoFactory extends DaoFactory {
    private static final String HIBERNATE_CONFIGURATION_LOCATION="hibernate-config/hibernate-mdd.cfg.xml";

    public MetaDataDaoFactory(ClusterSettings clusterSettings) {
        super(clusterSettings.getConfigDbMetadataSchema(), clusterSettings.getConfigDbJdbcEndpoint(), HIBERNATE_CONFIGURATION_LOCATION);
    }

    public MetaDataDaoFactory(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public MetaDataDaoFactory(String schema, JdbcEndpoint jdbcEndpoint) {
        super(schema, jdbcEndpoint, JdbcUrlUtils.guessApplicationName(), HIBERNATE_CONFIGURATION_LOCATION);
    }

    public MetaDataDao create() {
        return DaoFactory.<MetaDataDao> create(sessionFactory, MetaDataDaoImpl.class);
    }

}
