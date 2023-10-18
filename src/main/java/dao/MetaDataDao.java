package dao;

import org.hibernate.SessionFactory;

public class MetaDataDao extends Dao{
    protected MetaDataDao(SessionFactory sessionFactory) {
        super(sessionFactory);
    }


}
