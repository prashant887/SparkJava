package streaming;

import common.SparkStreamingExtract;
import common.SparkStreamingTransform;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class KafkaStreaming {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setAppName("SparkDstream")
                .setMaster("local[*]");

        //JavaSparkContext sc = new JavaSparkContext(conf);

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(10000));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("fromOffsets","earliest");
        kafkaParams.put("group.id", "group-1");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("supercollider");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream (
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );



        JavaDStream<SparkStreamingExtract.MessageTuple> partOffset=stream.map(
                new SparkStreamingExtract.ExtractPartitionAndOffset()
        );

        JavaPairDStream<String,String> message=partOffset.mapToPair(
                new SparkStreamingExtract.MessageTupleToPair()
        );

        /*message.foreachRDD(
                rdd->{
                    rdd.foreach(m->{
                        System.out.println("PAIR :"+m._1+" "+m._2);
                    });
                }
        );*/

        JavaDStream<Tuple2<String,Long>> mapMessage=message.mapPartitions(new SparkStreamingTransform.ToParquet());

        /*
        mapMessage.foreachRDD(rdd->{
            rdd.foreach(m->{
                System.out.println("Mapped Result :"+m._1+" "+m._2);
            });
        });
*/
        mapMessage.foreachRDD(makeLoadFunction());

        /*

         partOffset.foreachRDD(x->x.foreach(
                messageTuple -> {
                    System.out.println("Message Part Time:"+messageTuple.message+" "+messageTuple.partitionAndoffset+" "+messageTuple.timestamp);
                }
        ));

        stream.foreachRDD(rdd -> {
            System.out.println("--- New RDD with " + rdd.partitions().size()
                    + " partitions and " + rdd.count() + " records");
            rdd.foreach(record -> System.out.println("Key = " +record.key()+" Message :"+record.value()+" Time "+record.timestamp()));
        });

        JavaPairDStream<String, Tuple2<String,Long>> pairData= stream.mapToPair(
                record->new Tuple2<>(record.key(), new Tuple2<>(record.value(),record.timestamp()))
        );


         JavaDStream<Tuple3<String,String,Long>> pair=stream.map(record-> new Tuple3<>(record.key(), record.value(), record.timestamp()));

        pair.foreachRDD(rdd->{
            rdd.foreach(r->{
                System.out.println(r._1()+" "+r._2()+" "+r._3());
            });
        });

        JavaPairDStream<String, String> pairData=
                stream.map(new SparkStreamingExtract.ExtractPartitionAndOffset())
                        .mapToPair(new SparkStreamingExtract.MessageTupleToPair());


        pairData.mapPartitions(x->{
            System.out.println(x);
        });



        stream.foreachRDD(rdd -> {
            System.out.println("--- New RDD with " + rdd.partitions().size()
                    + " partitions and " + rdd.count() + " records");
            rdd.foreach(record -> System.out.println("Key = " +record.key()+" Message :"+record.value()));
        });
*/
        stream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            rdd.foreachPartition(consumerRecords -> {
                OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                System.out.println(
                        o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
            });
        });
        ssc.start();
        ssc.awaitTermination();


    }

    private static  void  KafkaMessages (ConsumerRecord<String, String> record) {
        System.out.println(record.timestamp() +"\n"+ record.offset()+"\n"+ record.partition()+"\n"+record.key()+"\n"+record.value());
    }

    private static SparkStreamingTransform.LoadParquetToDatabase makeLoadFunction() {
        return new SparkStreamingTransform.LoadParquetToDatabase();
    }
}
