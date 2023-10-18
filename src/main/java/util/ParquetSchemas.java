package util;

import common.HCatException;
import config.SparkJobSettings;
import etl.PartitionColumn;
import hcat.HCatalogFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import hcat.HCatalogException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetSchemas {

    private static final Logger log = LoggerFactory.getLogger(ParquetSchemas.class);

    private final Map<String, MessageType> schemas = new HashMap<>();

    public ParquetSchemas(SparkJobSettings jobSettings, CollectorsMetadata collectorIdMetadata) throws HCatException {
        Configuration conf = HCatalogFactory.getHcatConfiguration(jobSettings.clusterSettings);




    }

    ParquetSchemas(Map<String, MessageType> schemas) {
        this.schemas.putAll(schemas);
    }

    /**
     * Returns the parquet schema for the given table. The table name must be non-null. Null will be returned if there is no
     * parquet schema for a given table.
     *
     * @param table
     * @return The parquet schema
     * @throws IllegalArgumentException
     *            if table is null
     */
    public MessageType getParquetSchema(String table) throws IllegalArgumentException {
        if (table == null) {
            throw new IllegalArgumentException("Table name must non-null");
        }

        MessageType schema = schemas.get(table);
        if (schema == null) {
            log.debug("{} not found in cache. Will try w/ lower case.", table);
            schema = schemas.get(table.toLowerCase());
        }
        return schema;
    }


    /**
     * Checks if the schema matches the one held in the class. The parquet schema name, field names, field types, and field order
     * are compared.
     *
     * @param table
     *           table name to check the schema against
     * @param schema
     *           the schema to check
     * @return True if the schema matches, false otherwise
     * @throws IllegalArgumentException
     *            if either parameter is null
     */
    public boolean matches(String table, MessageType schema) throws IllegalArgumentException {
        if (table == null || schema == null) {
            throw new IllegalArgumentException("Table name and schema must non-null");
        }

        MessageType cachedSchema = getParquetSchema(table);

        boolean result = schema.equals(cachedSchema);
        if (!result) {
            log.warn(
                    "Schema does not match for {}! Generated parquet files may be invalid and possibly cause Impala errors.\ncached: {}, latest: {}",
                    table, cachedSchema, schema);
        }

        return result;
    }

    /**
     * Checks if the schema is for the updated partitioning scheme (pa__arrival_period)
     * TODO: temp change for repartitioning
     */
    static public boolean isNewPartitionScheme(MessageType schema) {
        // Tables that have migrated to the new partitioning scheme have pa__arrival_day as a regular table column
        return schema.containsField(PartitionColumn.PA__ARRIVAL_DAY.getName());
    }

    /**
     * Returns a list that contains the table name and period type
     */
    public List<Pair<String, String>> getTablePeriodTypes() {
        List<Pair<String, String>> periodTypes = new ArrayList<>();
        for (Map.Entry<String, MessageType> schemaSet : schemas.entrySet()) {
            if (isNewPartitionScheme(schemaSet.getValue())) {
                periodTypes.add(Pair.of(schemaSet.getKey(), "month"));
            } else {
                periodTypes.add(Pair.of(schemaSet.getKey(), "day"));
            }
        }
        return periodTypes;
    }
}