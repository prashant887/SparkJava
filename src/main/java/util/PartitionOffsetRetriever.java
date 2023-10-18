package util;


import bundle.BundleData;
import com.google.common.collect.ImmutableList;
import common.ClusterSettings;
import load.JdbcConnector;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.MDC;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;


/**
 * Returns starting partition(s) and offset(s). The source can be either from the DB or from an argument. In both cases, they are
 * checked against the topic's min and max partition/offsets for validity.
 */
public class PartitionOffsetRetriever {


    private static final String KAFKA_PARTITION = BundleData.ColumnName.KAFKA_PARTITION.toLowerCase();
    private static final String PARTITION_OFFSET = BundleData.ColumnName.KAFKA_PARTITION_OFFSET.toLowerCase();

    private final ClusterSettings clusterSettings;
    private final Map<String, String> topics;

    public PartitionOffsetRetriever(ClusterSettings clusterSettings, Map<String, String> topics) {
        this.clusterSettings = clusterSettings;
        this.topics = topics;
        MDC.put("topic", appendedTopics(topics));

    }

    private String appendedTopics(Map<String, String> topics) {
        return StringUtils.join(topics.values(), " ");
    }

    /**
     * Returns starting partition(s) and offset(s). Partitions and offsets are read from the DB.
     */

    Connection getConnection() throws SQLException {
        long start = System.currentTimeMillis();
        JdbcConnector jdbcConnector = new JdbcConnector();
        Connection conn = jdbcConnector.getConnection(clusterSettings.getEtlJdbcEndpoint());
        return conn;
    }

    public Map<String, List<PartitionOffset>> getPartitionAndOffsetFromArgs(String partitions, String offsets) {
        List<PartitionOffset> partitionAndOffset = new ArrayList<>();

        String[] parts = partitions.split(",");
        String[] offs = offsets.split(",");

        if (parts.length == offs.length) {
            for (int i = 0; i < parts.length; i++) {
                partitionAndOffset.add(new PartitionOffset(Integer.parseInt(parts[i]), Long.parseLong(offs[i])));
            }
        } else {
            String msg = "Mismatch for the number of partitions and offsets.";
            throw new IllegalArgumentException(msg);
        }
        Map<String, List<PartitionOffset>> partitionAndOffsetInfo = new HashMap<>();
        try (KafkaConsumer<String, String> consumer = createKafkaConsumer(clusterSettings.getKafkaBrokerList())) {
            for (String topic : topics.values()) {
                List<KafkaPartitionInfo> kafkaPartitions = getPartitionsAndOffsetsFromKafka(topic, consumer);
                partitionAndOffsetInfo.put(topic, sanitizePartitions(kafkaPartitions, partitionAndOffset));
            }
        }
        return partitionAndOffsetInfo;
    }


    KafkaConsumer<String, String> createKafkaConsumer(String brokerList) {
        System.out.println("Creating Kafka consumer...");
        long start = System.currentTimeMillis();
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        System.out.println("Done creating Kafka consumer. Took {} ms" + (System.currentTimeMillis() - start));
        return consumer;
    }

    private static List<PartitionOffset> sanitizePartitions(List<KafkaPartitionInfo> kafkaPartitions,
                                                            List<PartitionOffset> partitionsFromExternal) {

        Map<Integer, PartitionOffset> partitionMap = new HashMap<>();
        for (PartitionOffset part : partitionsFromExternal) {
            partitionMap.put(part.getPartition(), part);
        }

        System.out.println("ETL will start reading from:");
        List<PartitionOffset> parts = new ArrayList<>();
        Set<Integer> kafkaPartitionSet = new HashSet<>();
        for (KafkaPartitionInfo partitionInfo : kafkaPartitions) {
            kafkaPartitionSet.add(partitionInfo.partition);
            if (partitionMap.containsKey(partitionInfo.partition)) {
                // Partition information was found in both Kafka and the DB. Ensure that the offset is valid.
                Integer partition = partitionInfo.partition;

                Long externalOffset = partitionMap.get(partition).getOffset();
                Long offset = Math.max(partitionInfo.minOffset, Math.min(partitionInfo.maxOffset, externalOffset));
                Long diff = partitionInfo.maxOffset - offset;

                System.out.println(String.format("\tpartition {} \toffset {} \t(behind: {})", partition, offset, diff));
                parts.add(new PartitionOffset(partition, offset));
            } else {
                Long diff = partitionInfo.maxOffset - partitionInfo.minOffset;
                System.out.println(String.format("\tpartition {} \toffset {} \t(behind: {})", partitionInfo.partition, partitionInfo.minOffset, diff));
                parts.add(new PartitionOffset(partitionInfo.partition, partitionInfo.minOffset));
            }
        }
        checkPartitions(partitionMap.keySet(), kafkaPartitionSet);
        return parts;
    }



    private static void checkPartitions(Set<Integer> externalPartitions, Set<Integer> kafkaPartitions) {
        externalPartitions.removeAll(kafkaPartitions);
        if (!externalPartitions.isEmpty()) {
            System.out.println(String.format(
                    "External partitions not found in Kafka topic. These partitions were not found in Kafka: {}\n\tWill attempt to exit streaming job.",
                    externalPartitions));
            throw new RuntimeException("Partitions appear to be missing from the Kafka topic!");
        }
    }

    List<KafkaPartitionInfo> getPartitionsAndOffsetsFromKafka(String topic, KafkaConsumer<String, String> consumer) {
        System.out.println("Getting Kafka partition and offset info...");
        long start = System.currentTimeMillis();
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        Map<Integer, KafkaPartitionInfo> partitionsAndOffsets = new TreeMap<>();

        for (PartitionInfo partitionInfo : partitionInfos) {
            int partition = partitionInfo.partition();
            TopicPartition tp = new TopicPartition(topic, partition);

            consumer.assign(Arrays.asList(tp));
            consumer.seekToBeginning(ImmutableList.of(tp));
            long min = consumer.position(tp);

            consumer.seekToEnd(ImmutableList.of(tp));
            long max = consumer.position(tp);

            partitionsAndOffsets.put(partition, new KafkaPartitionInfo(partition, min, max));
        }

        System.out.println(String.format("Done getting Kafka partition and offset info. Took {} ms", System.currentTimeMillis() - start));
        System.out.println("Topic: {}"+topic);
        for (KafkaPartitionInfo info : partitionsAndOffsets.values()) {
            System.out.println(String.format("\tpartition: {} \tmin offset: {} \tmax offset: {}", info.partition, info.minOffset, info.maxOffset));
        }
        return new ArrayList<>(partitionsAndOffsets.values());
    }
    public static class PartitionOffset {
        private final int partition;
        private final long offset;

        PartitionOffset(int partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
        int getPartition() {
            return partition;
        }

        long getOffset() {
            return offset;
        }
    }

    static class KafkaPartitionInfo {
        public int partition;
        public long minOffset;
        public long maxOffset;

        public KafkaPartitionInfo(int partition, long minOffset, long maxOffset) {
            this.partition = partition;
            this.minOffset = minOffset;
            this.maxOffset = maxOffset;
        }
    }

}