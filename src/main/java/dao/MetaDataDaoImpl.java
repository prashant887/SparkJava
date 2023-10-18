package dao;

import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaDataDaoImpl extends MetaDataDao {

    private static final String HQL_META_SOURCE_INFO_BY_COLLECTOR = "FROM MetaSourceInfo WHERE srcCollectorIdType = :collectorId";

    private static final String HQL_COLLECTOR_DATALAKE_MAPPING_BY_COLLECTOR = "FROM CollectorDatalakeMapping c WHERE c.collectorId = :collectorId";

    private static final String HQL_COLLECTOR_DATALAKE_MAPPING = "FROM CollectorDatalakeMapping";

    public static final String ALL_COLLECTORS_ID = "*";

    private static final Logger log = LoggerFactory.getLogger(MetaDataDaoFactory.class);

    /**
     *(String)
     */
    private static final String ESCAPE_CHAR = "!";
    private static final String[] ESCAPE_CHARS = new String[] { "_", "%" };
    private static final String[] ESCAPE_REPLACEMENTS = new String[] { ESCAPE_CHAR + "_", ESCAPE_CHAR + "%" };

    /**
     * Escape clause for LIKE operators used in Hibernate queries
     */
    private static final String HQL_ESCAPE_CLAUSE = " escape '" + ESCAPE_CHAR +  "' ";


    protected MetaDataDaoImpl(SessionFactory sessionFactory) {
        super(sessionFactory);
    }



}
