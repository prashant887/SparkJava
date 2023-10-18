package entity;

import java.sql.Timestamp;
import java.util.LinkedHashSet;
import java.util.Set;

public class CollectorDatalakeMapping {

    private long id;
    private String collectorId;
    private String datalakeName;
    private String srcContentFormat;
    private String updatedBy;
    private Timestamp updatedTimestamp;
    /*
    private Set<MetaTableMappingInfo> metaTableMappingInfos = new LinkedHashSet<MetaTableMappingInfo>(0);

    public Set<MetaTableMappingInfo> getMetaTableMappingInfos() {
        return metaTableMappingInfos;
    }

     */

    public CollectorDatalakeMapping(long id, String collectorId, String datalakeName, String srcContentFormat) {
        this.id = id;
        this.collectorId = collectorId;
        this.datalakeName = datalakeName;
        this.srcContentFormat = srcContentFormat;
    }

    public CollectorDatalakeMapping (){

    }

    public CollectorDatalakeMapping(String collectorId, String datalakeName, String srcContentFormat) {
        this.collectorId = collectorId;
        this.datalakeName = datalakeName;
        this.srcContentFormat = srcContentFormat;
    }

    public String getDatalakeName() {
        return datalakeName;
    }

}
