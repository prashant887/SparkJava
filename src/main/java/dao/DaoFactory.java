package dao;

import common.JdbcEndpoint;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.JdbcUrlUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public abstract class DaoFactory implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DaoFactory.class);
    protected final SessionFactory sessionFactory;

    public DaoFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    public DaoFactory(String schema, JdbcEndpoint jdbcEndpoint, String hibernateConfigurationLocation) {
        this(schema, jdbcEndpoint, JdbcUrlUtils.guessApplicationName(), hibernateConfigurationLocation);
    }

    public DaoFactory(String schema, JdbcEndpoint jdbcEndpoint, String applicationName, String hibernateConfigurationLocation) {
        log.info("Connecting to database schema: {} ...", schema);
        // JDBC URL is logged by Hibernate
        this.sessionFactory = HibernateUtil.createSessionFactory(schema, jdbcEndpoint, applicationName, hibernateConfigurationLocation);
        log.debug("Connecting to database DONE.");
    }

    protected SessionFactory getSessionFactory() {
        return this.sessionFactory;
    }

    @SuppressWarnings("unchecked")
    static <T extends Dao> T create(SessionFactory sessionFactory, Class<?> classz) {
        log.debug("Creating Dao invocation proxy.");
        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(classz);

        T daoProxy;
        try {
            daoProxy = (T) factory.create(new Class<?>[] { SessionFactory.class }, new Object[] { sessionFactory },
                    new DaoProxy());
        } catch (ReflectiveOperationException | IllegalArgumentException e) {
            log.error("Error creating Dao proxy", e);
            if (e instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) e;
            }
            throw new RuntimeException(e);
        }
        return daoProxy;

    }

    @Override
    public void close() {
        this.sessionFactory.close();
    }

    protected static class DaoProxy implements MethodHandler {

        @Override
        public Object invoke(Object self, Method method, Method proceed, Object[] args) throws Throwable {
            boolean isSuccessfull = false;
            Throwable exc = null;
            try {
                Object result = proceed.invoke(self, args);
                isSuccessfull = true;
                return result;
            } catch (InvocationTargetException e) {
                exc = e.getTargetException();
                log.error("An error occurred during DAO operation: ", exc);
                throw exc;
            } finally {
                if (!isSuccessfull) {
                    log.warn("An exception was caught while trying to invoke a DAO's method - '" + method.getName()
                            + "'. The transaction will be automatically rolled back and all of the resources - closed.");
                    try {
                        if (!method.getName().contains("rollbackCurrentTransactionAndClearSession")) {
                            ((Dao) self).rollbackCurrentTransactionAndClearSession();
                        }
                    } catch (Exception e) {
                        log.warn("Failed to rollback transaction ", e);
                    }
                    try {
                        ((Dao) self).closeCurrentSession();
                    } catch (Exception e) {
                        log.warn("Failed to close session", e);
                    }
                }
            }
        }
    }
}
