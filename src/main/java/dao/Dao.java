package dao;

import org.apache.commons.lang.Validate;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.resource.transaction.spi.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public abstract class Dao {
    private static final Logger log = LoggerFactory.getLogger(Dao.class);

    private final SessionFactory sessionFactory;
    private Session currentSession;

    protected Dao(SessionFactory sessionFactory) {
        Validate.notNull(sessionFactory, "SessionFactory cannot be null");
        this.sessionFactory = sessionFactory;
    }

    public Session getCurrentSession() {
        return this.currentSession;
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    public void openSession() {
        closeCurrentSession();
        this.currentSession = sessionFactory.openSession();
    }

    public void openSessionWithTransaction() {
        if (this.currentSession != null) {
            commitTransactionAndCloseSession();
        }
        if (sessionFactory.isClosed()) {
            throw new IllegalStateException("The session factory is closed");
        }

        this.currentSession = sessionFactory.openSession();
        beginNewTransaction();
    }

    public void commitTransactionAndCloseSession() {
        if (this.currentSession != null) {
            try {
                commitCurrentTransaction();
            } catch (Exception e) {
                rollbackCurrentTransactionAndClearSession();
                throw e;
            } finally {
                closeCurrentSession();
            }
        }
    }

    public void closeCurrentSession() {
        log.trace("Closing hibernate session = {}", currentSession);
        if (currentSession != null) {
            currentSession.close();
            currentSession = null;
        }
    }

    public void beginNewTransaction() {
        if (this.currentSession != null) {
            currentSession.beginTransaction();
        }
    }

    public void commitCurrentTransaction() {
        if (this.currentSession != null) {
            Transaction transaction = this.currentSession.getTransaction();
            if (transaction != null && transaction.getStatus() == TransactionStatus.ACTIVE) {
                transaction.commit();
            }
        }
    }

    public void rollbackCurrentTransactionAndClearSession() {
        if (this.currentSession != null) {
            Transaction transaction = this.currentSession.getTransaction();
            if (transaction != null) {
                transaction.rollback();
            }
            this.currentSession.clear();
        }
    }

    /**
     * Persists a new or modified record
     *
     * <p>
     * If the entity is an record, retrieved by any of the lookup methods, it will be updated. If the entity is a newly
     * created object from the com.vmware.ph.analytics.metadata.entity package, it will be added to the database.
     *
     * <p>
     * No-op in case of no change.
     *
     * @param entity
     *           an 'active record' or a new object from the com.vmware.ph.analytics.metadata.entity package
     */
    public void saveOrUpdate(Object entity) {
        getCurrentSession().saveOrUpdate(entity);
    }

    public void save(Object entity) {
        getCurrentSession().save(entity);
    }

    public void persist(Object entity) {
        getCurrentSession().persist(entity);
    }

    public Object saveAndRetrieve(Object entity) {
        getCurrentSession().save(entity);
        return entity;
    }

    public void delete(Collection<? extends Object> objects) {
        for (Object object : objects) {
            delete(object);
        }
    }

    public void delete(Object object) {
        getCurrentSession().delete(object);
    }
}
