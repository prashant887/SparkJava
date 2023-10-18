import common.ClusterSettings;
import common.ConfigurationProvider;
import org.junit.Before;
import org.junit.Test;
import util.PartitionOffsetRetriever;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionOffsetRetrieverTest {

    private static String COLLECTOR_ID = "collectorId";
    private static String TOPIC = "topic";
    private static String HISTORY = "history";
    private static String CHUNK = "chunk";
    private static Map<String, String> topics;
    private static ClusterSettings CLUSTER_SETTINGS = ConfigurationProvider.create(ConfigurationProvider.Deployment.DEV).getClusterSettings();

    @Before
    public void setup() {
        topics = new HashMap<>();
        topics.put(COLLECTOR_ID, TOPIC);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getPartitionAndOffsetFromArgs_morePartitions() {
        PartitionOffsetRetriever retriever = new PartitionOffsetRetriever(CLUSTER_SETTINGS, topics);
        System.out.println(CLUSTER_SETTINGS);
        Map<String, List<PartitionOffsetRetriever.PartitionOffset>> parts=retriever.getPartitionAndOffsetFromArgs("0,1,2,3,4", "0,1");

        System.out.println(parts.keySet());
    }

    @Test
    public void getPartitionAndOffsetFromArgsPart() {
        PartitionOffsetRetriever retriever = new PartitionOffsetRetriever(CLUSTER_SETTINGS, topics);
        System.out.println(CLUSTER_SETTINGS.getKafkaBrokerList());

        Map<String, List<PartitionOffsetRetriever.PartitionOffset>> parts=retriever.getPartitionAndOffsetFromArgs("0,1,2,3,4", "0,1,2,3,4");

        System.out.println(parts.keySet());
    }

}
