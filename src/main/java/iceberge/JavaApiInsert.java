package iceberge;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class JavaApiInsert {

    /*
    https://tabular.io/blog/java-api-part-1/
     */

    public static void main(String[] args) throws IOException {

        Map<String, String> properties = new HashMap<>();

        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.hadoop.HadoopCatalog");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "/Users/pl/iceberg/warehouse/wh");

        HadoopCatalog catalog = new HadoopCatalog();
        Configuration conf = new Configuration();
        catalog.setConf(conf);
        catalog.initialize("demo", properties);

        Schema schema = new Schema(
                Types.NestedField.optional(1, "event_id", Types.StringType.get()),
                Types.NestedField.optional(2, "username", Types.StringType.get()),
                Types.NestedField.optional(3, "userid", Types.IntegerType.get()),
                Types.NestedField.optional(4, "api_version", Types.StringType.get()),
                Types.NestedField.optional(5, "command", Types.StringType.get())
        );

        Namespace webapp = Namespace.of("webapp");
        TableIdentifier name = TableIdentifier.of(webapp, "user_events");

        Table table;
        if (! catalog.tableExists(name)) {
              table=catalog.createTable(name, schema, PartitionSpec.unpartitioned());
        }
        else
        {
              table=catalog.loadTable(name);
        }

        GenericRecord record=GenericRecord.create(schema);

        ImmutableList.Builder<GenericRecord> builder = ImmutableList.builder();
        builder.add(record.copy(ImmutableMap.of("event_id", UUID.randomUUID().toString(), "username", "Bruce", "userid", 1, "api_version", "1.0", "command", "grapple")));
        builder.add(record.copy(ImmutableMap.of("event_id", UUID.randomUUID().toString(), "username", "Wayne", "userid", 1, "api_version", "1.0", "command", "glide")));
        builder.add(record.copy(ImmutableMap.of("event_id", UUID.randomUUID().toString(), "username", "Clark", "userid", 1, "api_version", "2.0", "command", "fly")));
        builder.add(record.copy(ImmutableMap.of("event_id", UUID.randomUUID().toString(), "username", "Kent", "userid", 1, "api_version", "1.0", "command", "land")));
        ImmutableList<GenericRecord> records = builder.build();

        String filepath = table.location() + "/data/" + UUID.randomUUID() +".snappy.parquet";

        System.out.println("Writing Data to :"+filepath);

        OutputFile file = table.io().newOutputFile(filepath);

        //Write Parquet Files
        DataWriter<GenericRecord> dataWriter =
                Parquet.
                        writeData(file)
                        .set("write.parquet.compression-codec", "SNAPPY")
                        .schema(schema)
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .overwrite()
                        .withSpec(PartitionSpec.unpartitioned())
                        .build();


        try {
            for (GenericRecord data : records) {
                dataWriter.write(data);
            }


        } finally {
            dataWriter.close();
        }

        //Appending a File to an Iceberg Table , convert files to parquet files

        DataFile dataFile = dataWriter.toDataFile();

        Table tbl = catalog.loadTable(name);
        tbl.newAppend().appendFile(dataFile).commit();


        CloseableIterable<Record> result = IcebergGenerics.read(tbl).build();
        for (Record r: result) {
            System.out.println(r);
        }

    }


}
