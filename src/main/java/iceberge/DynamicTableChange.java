package iceberge;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;

public class DynamicTableChange {

    public static void main(String[] args) throws IOException {

        Map<String, String> properties = new HashMap<>();

        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.hadoop.HadoopCatalog");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "/Users/pl/iceberg/warehouse/wh");

        HadoopCatalog catalog = new HadoopCatalog();
        Configuration conf = new Configuration();
        catalog.setConf(conf);
        catalog.initialize("demo", properties);

        Schema schema = new Schema(
                Types.NestedField.optional(1, "hotel_id", Types.LongType.get()),
                Types.NestedField.optional(2, "hotel_name", Types.StringType.get()),
                Types.NestedField.optional(3, "customer_id", Types.IntegerType.get()),
                Types.NestedField.optional(4, "arrival_date", Types.DateType.get()),
                Types.NestedField.optional(5, "departure_date", Types.DateType.get())

        );

        System.out.println(schema.columns());

        Namespace webapp = Namespace.of("bookings");
        TableIdentifier name = TableIdentifier.of(webapp, "rome_hotels");



        Table table;
        if (! catalog.tableExists(name)) {
            table=catalog.createTable(name, schema, PartitionSpec.unpartitioned());
            GenericRecord record=GenericRecord.create(schema);

            //ImmutableList.Builder<GenericRecord> builder = ImmutableList.builder();
            List<GenericRecord> records=new ArrayList<>();

            records.add(record.copy(Map.of("hotel_id",1L,"hotel_name","Taj","customer_id",100,"departure_date",LocalDate.parse("2023-04-12"),"arrival_date",LocalDate.parse("2023-04-08"))));
            records.add(record.copy(Map.of("hotel_id",2L,"hotel_name","WestEnd","customer_id",102,"departure_date",LocalDate.parse("2023-05-09"),"arrival_date",LocalDate.parse("2023-05-05"))));
            records.add(record.copy(Map.of("hotel_id",3L,"hotel_name","ITC","customer_id",105,"departure_date",LocalDate.parse("2023-03-25"),"arrival_date",LocalDate.parse("2023-03-20"))));
            records.add(record.copy(Map.of("hotel_id",4L,"hotel_name","Ashok","customer_id",107,"departure_date",LocalDate.parse("2023-02-15"),"arrival_date",LocalDate.parse("2023-02-10"))));

            System.out.println("Adding New Data ");
            AddData(records,table);
        }
        else
        {
            table=catalog.loadTable(name);
        }



        table.updateSchema().
                addColumn("hotel_geo_id", Types.LongType.get()).
                commit();




       // ImmutableList.Builder<GenericRecord> updateBuilder = ImmutableList.builder();

        Schema updateSchema=table.schema();

        GenericRecord newRecord=GenericRecord.create(updateSchema);

        List<GenericRecord> updatedRecords = new ArrayList<>();


        updatedRecords.add(newRecord.copy(
                Map.of(
                        "hotel_id",5L,
                        "hotel_name","Windsor",
                        "customer_id",108,
                        "arrival_date",LocalDate.parse("2023-06-10"),
                        "departure_date",LocalDate.parse("2023-06-15"),
                        "hotel_geo_id",200L
                )
        ));


        System.out.println("Update New Data ");
        AddData(updatedRecords,table);

        TableScan scan = table.newScan();

        Iterable<CombinedScanTask> tasks = scan.planTasks();

        tasks.forEach(
                x-> x.files().forEach(f-> System.out.println(f.file()))
        );

        table.snapshots().forEach(x->{
            System.out.println(x);
        });

        Transaction t = table.newTransaction();
t.newDelete().deleteFromRowFilter(Expressions.equal("hotel_id", 5L)).commit();

        SparkActions.get().deleteOrphanFiles(table);


    }



    static void AddData(List<GenericRecord> records, Table table) throws IOException {

        Schema schema=table.schema();


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
            dataWriter.write(records);
            /*
            for (GenericRecord data : records) {
                dataWriter.write(data);
            }

             */


        } finally {
            dataWriter.close();
        }

        //Appending a File to an Iceberg Table , convert files to parquet files

        DataFile dataFile = dataWriter.toDataFile();

        table.newAppend().appendFile(dataFile).commit();

        System.out.println("Reading Records");
        CloseableIterable<Record> result = IcebergGenerics.read(table).build();
        for (Record r: result) {
            System.out.println(r);
        }

        System.out.println("Done Reading");

    }

}
