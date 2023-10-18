package dao;

import entity.GdprCleanValue;
import org.hibernate.SessionFactory;

import java.util.List;

public abstract class GdprDataDao extends Dao {

    protected GdprDataDao(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public abstract List<GdprCleanValue> getCleanValues(String collectorId);

    public abstract GdprCleanValue getCleanValue(String piiValue);

    public abstract void updateCollectors(String piiValue, String collectorId);


    /**
     * Returns all GDPR clean values by given collector id.
     */

}
