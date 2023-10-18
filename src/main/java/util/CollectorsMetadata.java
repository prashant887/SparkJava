package util;

import dal.MetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class CollectorsMetadata implements Serializable {

    private static final long serialVersionUID = 6840453657605810233L;

    private Map<String, MetaData> metadataMap = new HashMap<>();

    public void put(String collectorId, MetaData metadata) {
        metadataMap.put(collectorId, metadata);
    }

}
