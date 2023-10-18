import api.InputContext;
import api.ParseException;
import api.Record;
import cdf.StreamingJsonExtractor;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class StreamingJsonExtractorTest {

    private static String BUNDLE_ID = "test_bundleId";
    private static String FILE_PATH = "test_filePath";

    @Test
    public void testExtractingArraysOfPrimitives() throws ParseException, IOException {
        String jsonStr =
                "{\"pa__metadata\":{\"client\":{\"ua-browser\":\"Chrome\",\"ua-os-version\":\"8.1\"}}}," // irrelevant meta data for this test
                        + "{"
                        + "\"namespace\":\"GENERIC_VPD\","
                        + "\"namespaceId\":5,"
                        + "\"data\":[0, -80, 0, 60, 0, 0, 64, 0, 0, 8, 0, -128],"
                        + "\"@type\":\"vim.host.ScsiLun.DurableName\""
                        + "}";

       /* jsonStr =
                "{\"pa__metadata\":{\"collection\":"
                        + "{\"collector\":\"ph_streaming_etl.1_0\",\"received-time\":\"1471478400444\",\"external\":\"false\",\"instance\":\"staging\"}}}"
                        + "{\"processing_start_time\":1471478400032,\"total_messages\":0,\"processing_delay\":289,\"total_delay\":290,"
                        + "\"@type\":\"pa__streaming_batch\",\"processing_end_time\":1471478400321,\"streaming_collector_id\":\"astro.1_0\","
                        + "\"batch_time\":1471478400000,\"scheduling_delay\":1,\"@id\":\"b11f0bdf-7a07-42ce-be2f-2a16cd0718bd\",\"submission_time\":1471478400031}";
*/

        System.out.println(jsonStr);

        List<Record> records = extractJsonData(jsonStr);
        System.out.println("String Json");
       records.forEach(System.out::println);
        String expectedMainObjectType = "vim.host.ScsiLun.DurableName";
        String expectedArrayRecordsType = "vim.host.ScsiLun.DurableName_data";
/*
        Record parentRecord = records.get(12);
        Record arrayEntryFirstElement = records.get(0);
        Record arrayEntryLastElement = records.get(11);

        System.out.println("\n");

        System.out.println(parentRecord);

        System.out.println("\n");

        System.out.println(arrayEntryFirstElement);
        System.out.println(arrayEntryLastElement);

        System.out.println("\n \n");

        System.out.println(parentRecord.getType());
        System.out.println(parentRecord.getMetrics());
        System.out.println(parentRecord.getMetrics().get(Record.RECOMMENDED_PRIMARY_KEY));
*/
    }

    private List<Record> extractJsonData(String jsonStr) throws ParseException, IOException {
        InputStream is = new ByteArrayInputStream(jsonStr.getBytes());
        InputContext ic = new InputContext(BUNDLE_ID, FILE_PATH);

        StreamingJsonExtractor streamJsonExtractor = new StreamingJsonExtractor();
        List<Record> records = streamJsonExtractor.extract(is, ic);
        return records;
    }
}
