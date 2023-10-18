package metadata;

import dao.GdprDataDaoFactory;
import dao.MetaDataDaoFactory;
import etl.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataProvider implements AutoCloseable{

    private static final Logger log = LoggerFactory.getLogger(MetadataProvider.class);

    private final MetaDataDaoFactory metaDataDaoFactory;
    private final GdprDataDaoFactory gdprDataDaoFactory;
    private static String FK_SUFFIX = (Constants.NAMESPACE_SEPARATOR + Constants.FOREIGN_KEY_COLUMN_SUFFIX)
            .toLowerCase();

    public MetadataProvider(MetaDataDaoFactory metaDataDaoFactory, GdprDataDaoFactory gdprDataDaoFactory) {
        this.metaDataDaoFactory = metaDataDaoFactory;
        this.gdprDataDaoFactory = gdprDataDaoFactory;
    }


    @Override
    public void close() throws Exception {

    }
}
