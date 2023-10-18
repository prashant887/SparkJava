package tests;

import org.apache.commons.io.Charsets;

public class SparkStreamingExtractTest {

    private static String TEST_MSG =
            "{\"pa__metadata\":{\"collection\":"
                    + "{\"collector\":\"ph_streaming_etl.1_0\",\"received-time\":\"1471478400444\",\"external\":\"false\",\"instance\":\"staging\"}}}"
                    + "{\"processing_start_time\":1471478400032,\"total_messages\":0,\"processing_delay\":289,\"total_delay\":290,"
                    + "\"@type\":\"pa__streaming_batch\",\"processing_end_time\":1471478400321,\"streaming_collector_id\":\"astro.1_0\","
                    + "\"batch_time\":1471478400000,\"scheduling_delay\":1,\"@id\":\"b11f0bdf-7a07-42ce-be2f-2a16cd0718bd\",\"submission_time\":1471478400031}";
    private static final byte[] TEST_MSG_BYTES = TEST_MSG.getBytes(Charsets.UTF_8);
    private static int PARTITION = 1;
    private static long OFFSET = 99L;
    private static String PARTITION_AND_OFFSET = PARTITION + " " + OFFSET;


}
