package config;

import common.ClusterSettings;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.CollectorsMetadata;
import util.SparkJobSettingsParser;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class SparkJobSettings implements Serializable {

    private static final long serialVersionUID = -5815916557229560065L;
    private static final Logger log = LoggerFactory.getLogger(SparkJobSettings.class);
    private SparkJobSettingsParser parser;

    /**
     * Sets the datalake based on the collector's schema. The value is only set if
     * the historyDb argument is not present.
     */
    public void setDatalakeIfNeeded(CollectorsMetadata collectorsMetadata) {
        /*
        if (clusterSettings.getHistoryDatabase() == null) {
            Set<String> datalakes = new HashSet<>();
            for (MetaData md : collectorsMetadata.getMetaDatas()) {
                datalakes.add(md.getDatalake());
            }
            // We thought it could be a good idea to have an ETL job handle multiple
            // collector IDs. The code was implemented, but never used. This was before
            // multiple datalakes existed.
            if (datalakes.size() == 1) {
                ClusterSettingsBuilder newSettingsBuilder = new ClusterSettingsBuilder(clusterSettings);
                newSettingsBuilder.setHistoryDatabase(datalakes.iterator().next());
                clusterSettings = newSettingsBuilder.build();
                parser.updateClusterSettings(clusterSettings);
            } else {
                throw new IllegalArgumentException("Multiple datalakes (from multiple collectors) configured for this job. Only 1 is expected");
            }
        } // else, the datalake was passed in as an argument and already set.

         */
    }

    /**
     * Indicates the purpose of the Spark job. Different jobs have different required parameters.
     */
    public static enum SparkPurpose {
        STREAMING, RE_ETL;
    }

    public String topics;
    public String collectorIds;
    public Map<String, String> topicsMap;
    public String environment;
    public boolean bootstrapping = false;
    public int maxBootstrappedColumns;
    public HashMap<TopicPartition, Long> fromOffsets;
    public HashMap<String, String> kafkaParams;
    public boolean cleanFirst = false;
    public long arrivalDay = 0;
    public String fileName;
    public int duration;
    public int rate;
    public String kryoBufferMax;
    public int gdprCacheSizeInBytes;
    public String stagingHdfsDir;
    public long startTime = Instant.now().toEpochMilli();
    public boolean officialRun = false;
    public boolean migrate = false;
    public boolean allowDuplicates = false;
    public boolean credentialRenewerEnabled = false;
    public int renewerPeriodHours = 12;

    public ClusterSettings clusterSettings;

    public SparkJobSettings(SparkPurpose sparkPurpose, String[] args) throws Exception {
      SparkJobSettingsParser  parser = new SparkJobSettingsParser(sparkPurpose, args);

        clusterSettings = parser.getClusterSettings();

        System.out.println("Cluster Setting :"+clusterSettings);


        System.out.println("Cluster Setting DB:"+clusterSettings.getHistoryDatabase());




    }
    }
