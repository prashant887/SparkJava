package streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_json;


import java.util.concurrent.TimeoutException;

public class StructuredStreaming {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        SparkSession spark=SparkSession
                .builder()
                .appName("StructuredStreaming")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataframe= spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "supercollider")
                .option("startingOffsets", "earliest") // From starting
                .load();



        //  personStringDF.show(20,0,true);



        dataframe.select(
                col("timestamp")
                        ,col("value").cast("string")
              //,  to_json(col("value").cast("string"))
                ).writeStream()
                .format("console")
                .option("truncate","false")
                .outputMode("append")
                .start()
                .awaitTermination();


    }
}
