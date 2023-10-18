package dao;

import common.JdbcEndpoint;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.Validate;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import util.JdbcUrlUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

/**
 * This is Singleton Class which creates or returns the existing SessionFactory
 *
 * NOTE : This class is not Thread Safe TODO: remove this as singleton in favour of something like:
 * HibernateSessionFactory factory = new HibernateSessionFactory(); autocloseable wrapper of SessionFactory
 *
 */
public class HibernateUtil {
    /**
     * Build the sessionFactory from Configure Object
     */
    public static SessionFactory createSessionFactory(String schema, JdbcEndpoint jdbcEndpoint, String applicationName,
                                                      String hibernateConfigurationLocation, URL... extraMappings) {
        Validate.notNull(schema, "The name of the schema is missing.");

        Configuration cfg = new Configuration().configure(hibernateConfigurationLocation);
        cfg.setProperty("hibernate.default_schema", schema);

        String jdbcUrl = JdbcUrlUtils.addApplicationName(jdbcEndpoint.getUrl(), applicationName);
        cfg.setProperty("hibernate.connection.url", jdbcUrl);

        cfg.setProperty("hibernate.connection.username", jdbcEndpoint.getUser());
        cfg.setProperty("hibernate.connection.password", jdbcEndpoint.getPassword());
        addMappings(cfg, extraMappings);

        return cfg.buildSessionFactory();

    }

    /**
     * Creates a Hibernate {@link SessionFactory} suitable for testing purposes. It has all necessary ORM mappings for
     * {@link }, but uses an in-memory database (in particular HSQLDB).
     */
    public static SessionFactory createTestSessionFactory(String schema, URL... extraMappings) {
        Configuration cfg = new Configuration().configure("hibernate-config/test-hibernate.cfg.xml");
        String connectionUrl = "jdbc:hsqldb:mem:" + schema + ";ifxeists=true;sql.syntax_pgs=true;shutdown=true;";
        cfg.setProperty("hibernate.connection.url", connectionUrl);
        addMappingsWithAddedConstraints(cfg);
        addMappings(cfg, extraMappings);
        return cfg.buildSessionFactory();
    }

    private static void addMappings(Configuration cfg, URL... mappings) {
        for (URL mappingUrl : mappings) {
            cfg.addURL(mappingUrl);
        }
    }

    /**
     * Adds mappings modified by adding constraints. Adding constraints is possible only in testing because otherwise the
     * constrains clash with ones defined in Postgres. On the other hand, having constraints in testing allows unit tests
     * to check for violations.
     *
     * <p>
     * The modification is currently implemented as String.replace but if all constraints are added this way, XSLT will
     * be more appropriate.
     */
    private static void addMappingsWithAddedConstraints(Configuration cfg) {
        try {
            String xmlString = IOUtils.toString(HibernateUtil.class.getResource("/hibernate-config/TargetColumn.hbm.xml"),
                    Charsets.UTF_8);
            URL stringUrl = toUrl(addTargetColumnUniqueConstraint(xmlString));
            cfg.addURL(stringUrl);
        } catch (IOException e) {
            //throw new Bug("Bundled resource not found");
            System.out.println("Bundled Not Found "+e.getMessage());
        }
    }


    /**
     * Converts a string to a URL that returns that string on URL.openStream()
     *
     * @throws MalformedURLException
     *            not possible, as URL string is hardcoded
     */
    private static URL toUrl(String string) throws MalformedURLException {
        URLConnection conn = new URLConnection(null) {

            @Override
            public void connect() {
            }

            @Override
            public InputStream getInputStream() throws IOException {
                return IOUtils.toInputStream(string, Charsets.UTF_8);
            }
        };
        return new URL(null, "foo:", new URLStreamHandler() {

            @Override
            protected URLConnection openConnection(URL u) throws IOException {
                return conn;
            }
        });
    }

    /**
     * Injects (tgtTableName, tgtColumnName) unique constraint used for testing
     *
     * @param xmlString
     *           the target column Hibernate mapping XML as string
     * @return
     */
    private static String addTargetColumnUniqueConstraint(String xmlString) {
        String marker = "<property name=\"tgtTableName\"";
        xmlString = xmlString.replace(marker, "<properties name='key' unique='true'>" + marker);
        marker = "<property name=\"tgtColumnType\"";
        xmlString = xmlString.replace(marker, "</properties>" + marker);
        return xmlString;
    }

}
