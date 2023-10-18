package sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
public class SparkSQL {

    public static void main(String[] args) {
        SparkSession spark= SparkSession
                .builder()
                .appName("SparkSQL")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataframe=spark.read().parquet("/Users/pl/amazon_electronics_review.parquet");


        dataframe.show(20,0,true);
    }


}
