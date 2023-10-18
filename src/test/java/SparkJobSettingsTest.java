import config.SparkJobSettings;
import dal.MetaData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import util.CollectorsMetadata;
import util.SparkJobSettingsParser;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class SparkJobSettingsTest {

    private static String COLLECTOR_ID_OPT = SparkJobSettingsParser.COLLECTOR_ID_OPT;
    private static String ENVIRONMENT_OPT = SparkJobSettingsParser.ENVIRONMENT_OPT;
    private static String BOOTSTRAP_OPT = SparkJobSettingsParser.BOOTSTRAPPING_OPT;
    private static String META_DATA_SCHEMA_OPT = SparkJobSettingsParser.META_DATA_SCHEMA_OPT;
    private static String HISTORY_DB_OPT = SparkJobSettingsParser.HISTORY_DB_OPT;

    private static String COLLECTOR_ID = "test-collector-id";
    private static String ENVIRONMENT = "test";
    private static String META_DATA_SCHEMA = "testMdSchema";
    private static String HISTORY_DB = "testHistoryDb";
    private static String DATA_LAKE = "test_datalake";

    @Test
    public void set_datalake_from_schema() throws Exception {
        String[] args =
                new String[] { COLLECTOR_ID_OPT, COLLECTOR_ID, ENVIRONMENT_OPT, ENVIRONMENT, BOOTSTRAP_OPT,
                        "false", META_DATA_SCHEMA_OPT, META_DATA_SCHEMA };
        SparkJobSettings jobSettings = new SparkJobSettings(SparkJobSettings.SparkPurpose.STREAMING, args);

        System.out.println("getHistoryDatabase : "+jobSettings.clusterSettings.getHistoryDatabase());

        CollectorsMetadata collectorIdMetadata = new CollectorsMetadata();
        MetaData md = mock(MetaData.class);

        doReturn(DATA_LAKE).when(md).getDatalake();
        collectorIdMetadata.put("collector_1", md);
        jobSettings.setDatalakeIfNeeded(collectorIdMetadata);
        Assertions.assertEquals(DATA_LAKE.toLowerCase(), jobSettings.clusterSettings.getHistoryDatabase());
    }
}
