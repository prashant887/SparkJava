import common.ClusterSettings;
import common.SparkStreamingTransform;
import config.SparkJobSettings;
import dal.MetaData;
import entity.CollectorDatalakeMapping;
import entity.MetaTableMappingInfo;
import etl.Constants;
import etl.ExtractedMetrics;
import org.apache.commons.io.Charsets;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import scala.Tuple2;
import util.CollectorsMetadata;
import util.ExtractedMetricsParquetWriter;

import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

public class SparkStreamingTransformTest {

    private static String TABLE_NAME = "vatm";
    private static String TABLE_NAME_NEST = "vatm_nest";
    private static String COLLECTOR_ID = "prototyping-only.v0";
    private static String DATALAKE = "datalake";
    private static String DEFAULT_CONTENT_FORMAT = Constants.STREAMING_FORMAT;
    private static String INSTANCE_ID = "instnew";
    private static long PARTITION = 1;
    private static long OFFSET = 99L;
    private static String PARTITION_AND_OFFSET = PARTITION + " " + OFFSET;
    private static String TOPIC = "test-topic";
    private static long MERGED_SIZE = 666L;
    private static long MERGED_INTERNAL_ID = 99L;

    @Test
    public void toExtractedMetrics() throws Exception {
        String jsonString = "{\"pa__metadata\":{\"client\":{\"ua-browser\":"
                + "\"Python-requests\",\"ua-browser-version\":\"2.2.1\",\"ua-os\":\"Linux\",\"ua-os-version\":\"\"},"
                + "\"collection\":{\"received-time\":\"1458583754147\",\"collector\":\"prototyping-only.v0\","
                + "\"instance\":\"instnew\",\"external\":\"true\"}}}"
                + "{\"@id\": \"uuid\", \"@type\": \"vatm\", \"value1\": \"testvalue\"}"
                + "{\"@id\": \"uuid2\",\"@type\": \"vatm\", \"value1\": \"othervalue\"}";
        List<ExtractedMetrics> extracted = extract(jsonString);
        System.out.println(extracted);

    }

    private List<ExtractedMetrics> extract(String jsonString, String contentFormat) throws Exception {

        ExtractedMetricsParquetWriter pw = mock(ExtractedMetricsParquetWriter.class);
        ArgumentCaptor<List> ems = ArgumentCaptor.forClass(List.class);
        doNothing().when(pw).add(any(), ems.capture(), any());



        List<Tuple2<String, byte[]>> tuple = new ArrayList<>();
        tuple.add(new Tuple2<>(PARTITION_AND_OFFSET, jsonString.getBytes(Charsets.UTF_8)));

        CollectorsMetadata collectorIdMetadata = new CollectorsMetadata();
        collectorIdMetadata.put(COLLECTOR_ID, getMetaData(contentFormat));

        SparkJobSettings sparkJobSettings = mock(SparkJobSettings.class);
        sparkJobSettings.topics = TOPIC;
        sparkJobSettings.collectorIds = COLLECTOR_ID;
        sparkJobSettings.clusterSettings = mock(ClusterSettings.class);

        /*
        SparkStreamingTransform.ToParquet.extractMetrics(tuple.iterator(), null, collectorIdMetadata,
                sparkJobSettings, pw, null);

         */

        return ems.getAllValues().size() > 0 ? ems.getValue() : Collections.emptyList();

    }

    private MetaData getMetaData(String contentFormat, String tableName) {
        long miId = 0;
        long tcId = 0;


        CollectorDatalakeMapping collectorDatalakeMapping = new CollectorDatalakeMapping(COLLECTOR_ID, DATALAKE, contentFormat);
        Set<MetaTableMappingInfo> metaTableMappingInfoSet = new HashSet<>();

   /*
        Set<MetaTableMappingInfo> streamingMappingSet = getCommonMappingSet(tableName, collectorDatalakeMapping, contentFormat);

        streamingMappingSet.add(MetadataProvider.generateFixedMappingInfo(++miId, tableName, "value1", ++tcId, "string",
                false, collectorDatalakeMapping));
        metaTableMappingInfoSet.addAll(streamingMappingSet);

         */
        return new MetaData();

    }

    private MetaData getMetaData(String contentFormat) {
        return getMetaData(contentFormat, TABLE_NAME);
    }

        private List<ExtractedMetrics> extract(String jsonString) throws Exception {
        return extract(jsonString, DEFAULT_CONTENT_FORMAT);
    }
/*
    private Set<MetaTableMappingInfo> getCommonMappingSet(String tableName, CollectorDatalakeMapping collectorDatalakeMapping, String contentFormat) {
        Set<MetaTableMappingInfo> streamingMappingSet = MetadataProvider.getCommonMappingSet(tableName, new CollectorDatalakeMapping(COLLECTOR_ID, DATALAKE, contentFormat));
        long miId = 0;
        long tcId = 0;

        streamingMappingSet.add(MetadataProvider.generateFixedMappingInfo(++miId, tableName, "ua-browser", ++tcId,

                "string", false, collectorDatalakeMapping));
        streamingMappingSet.add(MetadataProvider.generateFixedMappingInfo(++miId, tableName, "ua-browser-version",
                ++tcId, "string", false, collectorDatalakeMapping));
        streamingMappingSet.add(MetadataProvider.generateFixedMappingInfo(++miId, tableName, "ua-os", ++tcId, "string",
                false, new CollectorDatalakeMapping(COLLECTOR_ID, DATALAKE, contentFormat)));
        streamingMappingSet.add(MetadataProvider.generateFixedMappingInfo(++miId, tableName, "ua-os-version", ++tcId,
                "string", false, collectorDatalakeMapping));

        return streamingMappingSet;
    }

 */
}
