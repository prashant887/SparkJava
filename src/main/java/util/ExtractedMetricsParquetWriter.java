package util;

import config.SparkJobSettings;
import etl.ExtractedMetrics;
import hadoop.HadoopConfigurationAdapter;
import load.BundleData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A wrapper class for Parquet Writers. Creates a Parquet Writer for every table/arrival combination that is to written to
 * parquet. Parquet files are written to HDFS when this class is closed or if a write causes the block size to exceed or read the
 * HDFS block size.
 */
public class ExtractedMetricsParquetWriter implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ExtractedMetricsParquetWriter.class);

    private static final String SEP = "__";
    private static final int DEFAULT_BLOCK_SIZE = 16 * 1024 * 1024;
    private static final int DEFAULT_PAGE_SIZE = 1 * 1024 * 1024;
    private static final int DEFAULT_DICTIONARY_PAGE_SIZE = 1 * 1024 * 1024;
    private static final String PARQ_BLOCK_SIZE = "parq_block_size";
    private static final String PARQ_PAGE_SIZE = "parq_page_size";
    private static final String PARQ_DICTIONARY_PAGE_SIZE = "parq_dictionary_page_size";
    private static final int SCHEMA_VER = 1;

    private final Map<String, ParquetWriter<ExtractedMetrics>> parquetWriters = new ConcurrentHashMap<>();
    private final List<ParquetInfo> parquetInfo = new ArrayList<>();
    private final ParquetSchemas parquetSchemas;
    private final SparkJobSettings jobSettings;
    private final Configuration conf;

    public ExtractedMetricsParquetWriter(ParquetSchemas parquetSchemas, SparkJobSettings jobSettings) {
        this.parquetSchemas = parquetSchemas;
        this.jobSettings = jobSettings;
        Configuration conf = new Configuration();
        HadoopConfigurationAdapter.setupConfiguration(conf, jobSettings.clusterSettings);
        this.conf = conf;
    }

    public void add(BundleData bundleData, List<ExtractedMetrics> extractedMetrics, String kafkaInfo) throws IOException {
        // TODO: Temp commented out (will also need to update to truncate to month
        //long arrivalPeriod = DateUtil.truncateToDayInSeconds(bundleData.getDownloadDate());

        for (ExtractedMetrics em : extractedMetrics) {
            long arrivalPeriod = getArrivalPeriod(bundleData.getDownloadDate(), em.getTarget().getTableName());
            String key = genParquetWriterKey(em, arrivalPeriod);
            if (parquetWriters.containsKey(key)) {
                ParquetWriter<ExtractedMetrics> writer = parquetWriters.get(key);
                writer.write(em);
            } else {
                String table = em.getTarget().getTableName();
                String collectorId = bundleData.getCollectorIdType();
                String filepath = ParquetStagingUtil.genParquetFilePath(jobSettings, table, arrivalPeriod, collectorId, kafkaInfo);
                ParquetWriter<ExtractedMetrics> writer = getParquetWriter(table, filepath);

                writer.write(em);
                parquetWriters.put(key, writer);
                parquetInfo.add(new ParquetInfo(arrivalPeriod, collectorId, SCHEMA_VER, table, filepath, getPartitionCol(table),
                        getPeriodType(table)));
            }
        }
    }

    /**
     * Returns the arrival period based on day or month. The parquet schema of the table is used to determine if
     * a table has monthly or daily partitioning.
     * TODO: remove after migration is complete
     */
    private long getArrivalPeriod(Date date, String table) {
        MessageType mt = parquetSchemas.getParquetSchema(table);
        if (mt == null) {
            String msg = String.format("Parquet schema not found for the %s table in cache!", table);
            log.error(msg);
            throw new RuntimeException(msg);
        }

        if (ParquetSchemas.isNewPartitionScheme(mt)) {
            return DateUtil.truncateToMonthInSeconds(date);
        }
        return DateUtil.truncateToDayInSeconds(date);
    }

    /**
     * Returns the partition column based on the table.
     * TODO: remove after migration is complete
     */
    private String getPartitionCol(String table) {
        MessageType mt = parquetSchemas.getParquetSchema(table);
        if (mt == null) {
            String msg = String.format("Parquet schema not found for the %s table in cache!", table);
            log.error(msg);
            throw new RuntimeException(msg);
        }

        if (ParquetSchemas.isNewPartitionScheme(mt)) {
            return "pa__arrival_period";
        }
        return "pa__arrival_day";
    }

    /**
     * Returns the partition column based on the table. TODO: remove after migration is complete
     */
    private String getPeriodType(String table) {
        MessageType mt = parquetSchemas.getParquetSchema(table);
        if (mt == null) {
            String msg = String.format("Parquet schema not found for the %s table in cache!", table);
            log.error(msg);
            throw new RuntimeException(msg);
        }

        if (ParquetSchemas.isNewPartitionScheme(mt)) {
            return "month";
        }
        return "day";
    }

    /**
     * Flushes the Parquet Writers to HDFS
     *
     * @throws IOException
     *            if an error occurs while closing the Parquet Writer
     */
    @Override
    public void close() throws IOException {
        for (ParquetWriter<ExtractedMetrics> writer : parquetWriters.values()) {
            writer.close();
        }
        parquetWriters.clear();
    }

    public List<ParquetInfo> getParquetInfo() {
        return parquetInfo;
    }

    private ParquetWriter<ExtractedMetrics> getParquetWriter(String table, String filepath) throws IOException {
        Path path = new Path(filepath);
        MessageType messageType = parquetSchemas.getParquetSchema(table);
        if (messageType == null) {
            String msg = String.format("Parquet schema not found for the %s table in cache!", table);
            log.error(msg);
            throw new RuntimeException(msg);
        }

        ExtractedMetricsWriteSupport writeSupport = new ExtractedMetricsWriteSupport(messageType);

        final int blockSize = Integer.getInteger(PARQ_BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
        final int pageSize = Integer.getInteger(PARQ_PAGE_SIZE, DEFAULT_PAGE_SIZE);
        final int dictionaryPageSize = Integer.getInteger(PARQ_DICTIONARY_PAGE_SIZE, DEFAULT_DICTIONARY_PAGE_SIZE);

        log.info("table: {}; blockSize: {}; pageSize: {}; dictionaryPageSize: {}; filePath: {}",
                table, blockSize, pageSize, dictionaryPageSize, filepath);

        // TODO: fix parameters. Selected something the IDE suggested to make it compile
        return new ParquetWriter<ExtractedMetrics>(path, writeSupport,
                CompressionCodecName.SNAPPY, // Impala uses snappy by default
                blockSize, // HDFS block size
                pageSize, // page size
                dictionaryPageSize, // dictionaryPageSize
                true, // enable dictionary encoding
                false, // disable validation using the schema
                ParquetProperties.WriterVersion.PARQUET_1_0, // see https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/cdh_ig_parquet.html
                conf);
    }

    private String genParquetWriterKey(ExtractedMetrics em, long arrivalPeriod) {
        return em.getTarget().getTableName() + SEP + arrivalPeriod;
    }
}
