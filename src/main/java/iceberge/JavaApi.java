package iceberge;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkCatalog;

import java.util.HashMap;
import java.util.Map;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;


public class JavaApi {

    public static void main(String[] args) {

        Configuration conf= new Configuration();


        String warehousePath = "/Users/pl/PycharmProjects/EUC-BI/SparkCodes/iceberg-warehouse";

        //String warehousePath = "/tmp/iceberg-warehouse";


        Map<String, String> properties = new HashMap<String, String>();
        properties.put("warehouse", warehousePath);

        HadoopCatalog catalog = new HadoopCatalog();
        catalog.setConf(conf);
        catalog.initialize("iceberg",properties);

        System.out.println("Cata Log :"+catalog.name());





        Schema schema = new Schema(
                required(1, "hotel_id", Types.LongType.get()),
                optional(2, "hotel_name", Types.StringType.get()),
                required(3, "customer_id", Types.LongType.get()),
                required(4, "arrival_date", Types.DateType.get()),
                required(5, "departure_date", Types.DateType.get())
        );

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("hotel_id")
                .build();

        TableIdentifier id = TableIdentifier.of("rome_hotels");



        TableIdentifier tab=TableIdentifier.of("table1");

        if (!catalog.tableExists(id)) {
              Table table=catalog.createTable(id,schema,spec);
        }
        System.out.println(catalog.listNamespaces());

        System.out.println(catalog.loadTable(id));

        System.out.println(catalog.loadTable(tab));


        System.out.println(catalog.loadTable(tab).schemas());

        System.out.println(catalog.loadTable(tab).schema());

        System.out.println(catalog.loadTable(id).schema());




    }


}
