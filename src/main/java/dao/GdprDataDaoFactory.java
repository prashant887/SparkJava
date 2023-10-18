package dao;

import common.ClusterSettings;
import common.JdbcEndpoint;
import org.hibernate.SessionFactory;
import util.JdbcUrlUtils;

public class GdprDataDaoFactory extends DaoFactory {

    private static final String HIBERNATE_CONFIGURATION_LOCATION="hibernate-config/hibernate-gdpr.cfg.xml";

    public GdprDataDaoFactory(ClusterSettings clusterSettings) {
        super(clusterSettings.getGdprDbSchema(), clusterSettings.getGdprDbJdbcEndpoint(), HIBERNATE_CONFIGURATION_LOCATION);
    }

    public GdprDataDaoFactory(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public GdprDataDaoFactory(String schema, JdbcEndpoint jdbcEndpoint) {
        super(schema, jdbcEndpoint, JdbcUrlUtils.guessApplicationName());
    }

    public GdprDataDao create() {
        return DaoFactory.<GdprDataDao> create(sessionFactory, GdprDataDaoImpl.class);
    }
}
