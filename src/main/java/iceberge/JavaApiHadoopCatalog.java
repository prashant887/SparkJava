package iceberge;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.hadoop.HadoopCatalog;

import org.apache.iceberg.types.Types;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;


import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.IcebergGenerics;

import org.apache.iceberg.expressions.Expressions;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JavaApiHadoopCatalog {

    public static void main(String[] args) {

        Map<String, String> properties = new HashMap<>();

        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.hadoop.HadoopCatalog");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "/Users/pl/iceberg/warehouse/wh");

        HadoopCatalog catalog = new HadoopCatalog();
        Configuration conf = new Configuration();
        catalog.setConf(conf);
        catalog.initialize("demo", properties);


        Schema schema = new Schema(
                Types.NestedField.required(1, "level", Types.StringType.get()),
                Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
                Types.NestedField.required(3, "message", Types.StringType.get()),
                Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get()))
        );

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .hour("event_time")
                .build();

        Namespace namespace = Namespace.of("webapp");
        TableIdentifier name = TableIdentifier.of(namespace, "logs");

        if (! catalog.tableExists(name)){
            catalog.createTable(name, schema, spec);

        }

        List<TableIdentifier> tables = catalog.listTables(namespace);
        System.out.println(tables);

        Table table=catalog.loadTable(name);


        /*
        Here insert data from Spark try with Java
        spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.2.0 --master local /Users/pl/PycharmProjects/EUC-BI/SparkCodes/iceBergeHadoopJavaApi.py

         */

        CloseableIterable<Record> result = IcebergGenerics.read(table).build();

        for (Record r: result) {
            System.out.println(r);
        }


        CloseableIterable<Record> resultFilter = IcebergGenerics.read(table)
                .where(Expressions.equal("level", "error"))
                .build();

        for (Record r: resultFilter) {
            System.out.println(r);
        }

        TableScan scan = table.newScan();

        TableScan filteredScan = scan.filter(Expressions.equal("level", "info")).select("message");

        Iterable<CombinedScanTask> resultScan = filteredScan.planTasks();

        resultScan.forEach(x->{
            System.out.println(x.estimatedRowsCount());
        });



        for (CombinedScanTask r: resultScan) {
            r.files().forEach(x->{
                System.out.println(x.file().path());
            });
        }


        CombinedScanTask task = resultScan.iterator().next();
        DataFile dataFile = task.files().iterator().next().file();
        System.out.println(dataFile);


    }
}
