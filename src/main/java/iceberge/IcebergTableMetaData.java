package iceberge;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class IcebergTableMetaData {

    static class MetaData {
        int id;

        String name;

        String dataType;

        public MetaData(int id, String name, String dataType) {
            this.id = id;
            this.name = name;
            this.dataType = dataType;
        }

        @Override
        public String toString() {
            return "MetaData{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", dataType='" + dataType + '\'' +
                    '}';
        }
    }


    public static void main(String[] args) {

        Map<String, String> properties = new HashMap<>();

        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.hadoop.HadoopCatalog");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "/Users/pl/iceberg/warehouse/wh");

        HadoopCatalog catalog = new HadoopCatalog();
        Configuration conf = new Configuration();
        catalog.setConf(conf);
        catalog.initialize("demo", properties);

        Namespace webapp = Namespace.of("bookings");
        TableIdentifier name = TableIdentifier.of(webapp, "rome_hotels");

        Table table=catalog.loadTable(name);



        Configuration configuration=new Configuration();

        FileIO io=new HadoopFileIO(configuration);


        StaticTableOperations staticTableOperations=new StaticTableOperations(
                "/Users/pl/iceberg/warehouse/wh/bookings/rome_hotels/metadata/v1.metadata.json",io
        );



        TableMetadata metadata = staticTableOperations.current();

        System.out.println(metadata.schema());




        Schema schema=table.schema();

        System.out.println(schema.asStruct());
        System.out.println(schema.caseInsensitiveFindField("customer_ids"));

        System.out.println(schema.findField("customer_id"));

        List<MetaData> metaDataList=new ArrayList<>();
        schema.columns().forEach(x->{
            metaDataList.add(new MetaData(x.fieldId(),x.name(),x.type().toString()));
        });

        metaDataList.forEach(System.out::println);

       // schema.columns().add(Types.NestedField.optional(7,"hotel_rating",Types.IntegerType.get()));

        System.out.println(schema.columns());



    }
}
