import config.SparkJobSettings;
import org.junit.jupiter.api.Test;
import util.SparkJobSettingsParser;

public class SparkJobSettingsParserTest {

    private static String COLLECTOR_ID_OPT = SparkJobSettingsParser.COLLECTOR_ID_OPT;
    private static String ENVIRONMENT_OPT = SparkJobSettingsParser.ENVIRONMENT_OPT;
    private static String BOOTSTRAP_OPT = SparkJobSettingsParser.BOOTSTRAPPING_OPT;
    private static String PARTITION_OPT = SparkJobSettingsParser.PARTITIONS_OPT;
    private static String OFFSET_OPT = SparkJobSettingsParser.OFFSETS_OPT;
    private static String CLEAN_FIRST_OPT = SparkJobSettingsParser.CLEAN_FIRST_OPT;
    private static String ARRIVAL_DAY_OPT = SparkJobSettingsParser.ARRIVAL_DAY_OPT;
    private static String META_DATA_SCHEMA_OPT = SparkJobSettingsParser.META_DATA_SCHEMA_OPT;
    private static String HISTORY_DB_OPT = SparkJobSettingsParser.HISTORY_DB_OPT;
    private static String TOPIC_OPT = SparkJobSettingsParser.TOPIC_OPT;
    private static String DUR_OPT = SparkJobSettingsParser.DURATION_OPT;

    private static String COLLECTOR_ID = "test-collector-id";
    private static String ENVIRONMENT = "test";
    private static String TOPIC = "ph_tst-" + COLLECTOR_ID;
    private static int PARTITION_FROM_DB = 0;
    private static int PARTITION_FROM_ARGS = 1;
    private static long OFFSET_FROM_DB = 99L;
    private static long OFFSET_FROM_ARGS = 100L;
    private static String META_DATA_SCHEMA = "testMdSchema";
    private static String HISTORY_DB = "testHistoryDb";
    private static String TEST_TOPIC = "test_topic";
    private static int DUR_SEC = 900;
    private static String DATA_LAKE = "test_datalake";


    @Test
    public void testStreamingParse_noArguments() throws Exception {
        String[] args = new String[] {};

        SparkJobSettingsParser parser = new SparkJobSettingsParser(SparkJobSettings.SparkPurpose.STREAMING, args);

        System.out.println(parser);
    }

@Test
    public void testStreamingParse_incorrectArguments() throws Exception {
        String[] args = new String[] { COLLECTOR_ID_OPT, COLLECTOR_ID, ENVIRONMENT_OPT, ENVIRONMENT };
        @SuppressWarnings("unused")
        SparkJobSettingsParser parser = new SparkJobSettingsParser(SparkJobSettings.SparkPurpose.STREAMING, args);
    System.out.println(parser);
    }

}
