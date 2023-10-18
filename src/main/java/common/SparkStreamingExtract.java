package common;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;


public class SparkStreamingExtract {


    public static class MessageTuple {
        public String partitionAndoffset;
        public String message;

        public Long timestamp;

        public MessageTuple(String partitionAndoffset, String message,Long timestamp) {
            this.partitionAndoffset = partitionAndoffset;
            this.message = message;
            this.timestamp=timestamp;
        }
    }

    public static class ExtractPartitionAndOffset implements Function<ConsumerRecord<String, String>, MessageTuple> {

        private static final long serialVersionUID = 6625642403199514097L;

        @Override
        public MessageTuple call(ConsumerRecord<String, String> msgAndMd) throws Exception {
            System.out.println("In Call Method of ExtractPartitionAndOffset "+msgAndMd);
            MessageTuple tuple = new MessageTuple(msgAndMd.partition() + " " + msgAndMd.offset(), msgAndMd.value(), msgAndMd.timestamp());
            return tuple;
        }
    }

    public static class MessageTupleToPair implements PairFunction<MessageTuple, String, String> {
        private static final long serialVersionUID = -7744166993421776136L;


        @Override
        public Tuple2<String, String> call(MessageTuple messageTuple) throws Exception {
            return new Tuple2<>(messageTuple.partitionAndoffset, messageTuple.message);
        }
    }

    public static class ExtractSequenceFileMetaData implements
            PairFunction<Tuple2<LongWritable, BytesWritable>, String, String> {

        private static final long serialVersionUID = -2199374971744118430L;

        private static final int EXPECTED_HEADER_SIZE = 4;

        private static final int PARTITION_IDX = 1;
        private static final int OFFSET_IDX = 3;
        private static final String DIGIT_REGEX = "\\d+";

        @Override
        public Tuple2<String, String> call(Tuple2<LongWritable, BytesWritable> record)  {
            String data = new String(record._2.getBytes());
            int index = data.indexOf('}');
            System.out.println(String.format("Data = [%s] Index = [%d]",data,index));

            // Create an array of the words from the partition and offset header
            String[] dataArray = data.substring(1, index).split("\\s");

            System.out.println(String.format("Data Array = "+ Arrays.toString(dataArray)));
            System.out.println(String.format("Length = [%d]",dataArray.length));

            if (dataArray.length != EXPECTED_HEADER_SIZE) {
                throw new RuntimeException("Unexpected partition and offset header array size: " + dataArray.length);
            }

            String partition = dataArray[PARTITION_IDX];
            String offset = dataArray[OFFSET_IDX];
            if (!partition.matches(DIGIT_REGEX) || !offset.matches(DIGIT_REGEX)) {
                throw new RuntimeException("Partition and offset expected to be a number! Partition: " + partition
                        + " Offset: " + offset);
            }

            String rawData = new String(Arrays.copyOfRange(record._2.getBytes(),index +1, record._2.getBytes().length));
            System.out.println("Raw Data : "+rawData);
            return new Tuple2<>(dataArray[1] + " " + dataArray[3], rawData);
        }
    }

}
