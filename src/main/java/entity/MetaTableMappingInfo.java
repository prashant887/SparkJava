package entity;

import java.sql.Timestamp;

public class MetaTableMappingInfo {

    private static final long serialVersionUID = 1L;

    private long id;

    private String srcEntityName;
    private String srcParentNode;
    private String expression;
    private String srcTransformation;
    private TargetColumn targetColumn;

    private String updatedBy;
    private Timestamp updatedTimestamp;
    private CollectorDatalakeMapping collectorDatalakeMapping;

    public CollectorDatalakeMapping getCollectorDatalakeMapping() {
        return collectorDatalakeMapping;
    }

    public void setCollectorDatalakeMapping(CollectorDatalakeMapping collectorDatalakeMapping) {
        this.collectorDatalakeMapping = collectorDatalakeMapping;
    }

    public MetaTableMappingInfo() {
    }

    public MetaTableMappingInfo(long id, String srcEntityName, String expression, String srcTransformation,
                                TargetColumn targetColumn, CollectorDatalakeMapping collectorDatalakeMapping) {
        this.id = id;
        this.srcEntityName = srcEntityName;
        this.expression = expression;
        this.srcTransformation = srcTransformation;
        this.targetColumn = targetColumn;
        this.collectorDatalakeMapping = collectorDatalakeMapping;
    }

    public MetaTableMappingInfo(long id, String srcEntityName, String expression,
                                String srcTransformation, TargetColumn targetColumn, String updatedBy, Timestamp updatedTimestamp, CollectorDatalakeMapping collectorDatalakeMapping) {
        super();
        this.id = id;
        this.srcEntityName = srcEntityName;
        this.expression = expression;
        this.srcTransformation = srcTransformation;
        this.targetColumn = targetColumn;
        this.updatedBy = updatedBy;
        this.updatedTimestamp = updatedTimestamp;
        this.collectorDatalakeMapping = collectorDatalakeMapping;
    }

}
