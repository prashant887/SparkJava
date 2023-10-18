package load;

import common.JdbcEndpoint;
import org.apache.commons.lang3.StringUtils;
import util.JdbcUrlUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcConnector {


    public Connection getConnection(String dbUrl, String username, String password) throws SQLException {
        dbUrl = JdbcUrlUtils.addApplicationName(dbUrl);

        if (dbUrl.startsWith("jdbc:impala:")) {
            // In a hadoop fat jar, the META-INF files required for automated driver discovery may not be available.
            try {
                Class.forName("com.cloudera.impala.jdbc41.Driver");
            } catch (ClassNotFoundException e) {
                throw new SQLException("Could not load impala driver class.", e);
            }
        }

        if (dbUrl.startsWith("jdbc:hive")) {
            // The Hive driver is used for bootstrapping queries to Hive and streaming ETL
            try {
                Class.forName("org.apache.hive.jdbc.HiveDriver");
            } catch (ClassNotFoundException e) {
                throw new SQLException("Could not load hive driver class.", e);
            }
        }
        String obfuscatedPassword = password != null ? StringUtils.repeat('*', password.length()) : "<no password>";
        long start = System.currentTimeMillis();
        Connection conn = DriverManager.getConnection(dbUrl, username, password);
        return conn;
    }

    public Connection getConnection(JdbcEndpoint jdbcEndpoint) throws SQLException {
        return getConnection(jdbcEndpoint.getUrl(), jdbcEndpoint.getUser(), jdbcEndpoint.getPassword());
    }
}
