import common.SparkStreamingExtract;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

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

    @Test
    public void extractPartitionAndOffset() throws Exception {
        ConsumerRecord<String, String> msgAndMd =
                new ConsumerRecord<String, String>("test_topic", PARTITION, OFFSET, "key", TEST_MSG);

        SparkStreamingExtract.ExtractPartitionAndOffset extractor = new SparkStreamingExtract.ExtractPartitionAndOffset();

        SparkStreamingExtract.MessageTuple tuple = extractor.call(msgAndMd);

        System.out.println(tuple.message);

        System.out.println(tuple.partitionAndoffset);


        Assertions.assertEquals(PARTITION_AND_OFFSET, tuple.partitionAndoffset);
        Assertions.assertEquals(TEST_MSG, tuple.message);
    }


    @Test
    public void stringTupleToPair() throws Exception {
        SparkStreamingExtract.MessageTuple strTuple = new SparkStreamingExtract.MessageTuple(PARTITION_AND_OFFSET, TEST_MSG,null);
        SparkStreamingExtract.MessageTupleToPair converter = new SparkStreamingExtract.MessageTupleToPair();

        Tuple2<String, String> tuple = converter.call(strTuple);

        System.out.println(tuple._1);

        System.out.println(tuple._2);

        Assertions.assertEquals(PARTITION_AND_OFFSET, tuple._1);
        Assertions.assertEquals(TEST_MSG, tuple._2);
    }

    @Test
    public void testExtractSequenceFile() {
        String header = "{Partition 1 Offset 99}";
        String msgBackup = header + TEST_MSG;
        LongWritable l = new LongWritable(1000L);
        BytesWritable b = new BytesWritable(msgBackup.getBytes());
        Tuple2<LongWritable, BytesWritable> data = new Tuple2<>(l, b);

        SparkStreamingExtract.ExtractSequenceFileMetaData extractor = new SparkStreamingExtract.ExtractSequenceFileMetaData();
        Tuple2<String, String> tuple = extractor.call(data);


        System.out.println(tuple._1);
        System.out.println(tuple._2);
        Assertions.assertEquals(PARTITION_AND_OFFSET, tuple._1);
        Assertions.assertEquals(TEST_MSG, tuple._2);
    }

    @Test
    public void testExtractSequenceFile_badHeader_tooFewWords() {
        String header = "{Partition 1 99}";
        String msgBackup = header + TEST_MSG;
        LongWritable l = new LongWritable(1000L);
        BytesWritable b = new BytesWritable(msgBackup.getBytes());
        Tuple2<LongWritable, BytesWritable> data = new Tuple2<>(l, b);

        System.out.println(msgBackup);

        SparkStreamingExtract.ExtractSequenceFileMetaData extractor = new SparkStreamingExtract.ExtractSequenceFileMetaData();
        extractor.call(data);
    }




}
