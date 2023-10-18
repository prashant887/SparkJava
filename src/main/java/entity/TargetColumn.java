package entity;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import util.AbbreviatedToStringStyle;

import java.io.Serializable;
import java.util.Date;

public class TargetColumn implements Serializable {

    private static final long serialVersionUID = 1L;

    private long id;
    //TODO: Change this to be a reference to TargetTable and configure Hibernate to load the appropriate object by its FK
    private String tgtTableName;
    private String tgtColumnName;
    private String tgtColumnType;
    private Integer tgtColumnLength;
    private Integer tgtColumnPrecision;
    private Boolean tgtColumnPrimaryKey;
    private String tgtColumnDescription;

    // The default value of FALSE indicates that VAC does not currently know if the column is used as fk
    //(although null might have been more appropriate).
    // Value of TRUE should be used when VAC knows for sure somebody is using the column as fk.
    // Review: https://reviewboard.eng.vmware.com/r/1361050/
    // Bug: https://bugzilla.eng.vmware.com/show_bug.cgi?id=2113561
    private Boolean isForeignKey = false;
    private String referencedTableName;

    private String updatedBy;
    private Date updatedTimestamp;
    private Date createdTimestamp;
    private Boolean isSensitive = false;
    private String anonymizationAlgorithm = "";
    private String tgtDatalakeName;

    public String getTgtDatalakeName() {
        return tgtDatalakeName;
    }

    public void setTgtDatalakeName(String tgtDatalakeName) {
        this.tgtDatalakeName = tgtDatalakeName;
    }

    public TargetColumn() {
    }

    public TargetColumn(long id, String tgtTableName, String tgtColumnName, String tgtColumnType,
                        String tgtColumnDescription, Boolean tgtColumnPrimaryKey, String tgtDatalakeName) {
        this.id = id;
        this.tgtTableName = tgtTableName.toLowerCase();
        this.tgtColumnName = tgtColumnName.toLowerCase();
        this.tgtColumnType = tgtColumnType;
        this.tgtColumnDescription = tgtColumnDescription;
        this.tgtColumnPrimaryKey = tgtColumnPrimaryKey;
        this.tgtDatalakeName = tgtDatalakeName;
    }

    public TargetColumn(String tgtTableName, String tgtColumnName, String tgtColumnType) {
        this.tgtTableName = tgtTableName;
        this.tgtColumnName = tgtColumnName;
        this.tgtColumnType = tgtColumnType;
    }

    public TargetColumn(TargetColumn column) {
        this(column.getId(), column.getTgtTableName(), column.getTgtColumnName(), column.getTgtColumnType(),
                column.getTgtColumnLength(), column.getTgtColumnPrecision(), column.getTgtColumnPrimaryKey(),
                column.getIsForeignKey(), column.getReferencedTableName(), column.getTgtColumnDescription(),
                column.getUpdatedBy(), column.getUpdatedTimestamp(), column.getCreatedTimestamp(), column.getIsSensitive(),
                column.getAnonymizationAlgorithm(), column.tgtDatalakeName);
    }

    public TargetColumn(long id, String tgtTableName, String tgtColumnName, String tgtColumnType,
                        Integer tgtColumnLength, Integer tgtColumnPrecision, Boolean tgtColumnPrimaryKey, Boolean isForeignKey,
                        String referencedTableName, String tgtColumnDescription, String updatedBy, Date updatedTimestamp,
                        Date createdTimestamp, boolean isSensitive, String anonymizationAlgorithm, String tgtDatalakeName) {
        this.id = id;
        this.tgtTableName = tgtTableName.toLowerCase();
        this.tgtColumnName = tgtColumnName.toLowerCase();
        this.tgtColumnType = tgtColumnType;
        this.tgtColumnLength = tgtColumnLength;
        this.tgtColumnPrecision = tgtColumnPrecision;
        this.tgtColumnPrimaryKey = tgtColumnPrimaryKey;
        this.isForeignKey = isForeignKey;
        this.referencedTableName = referencedTableName;
        this.tgtColumnDescription = tgtColumnDescription;
        this.updatedBy = updatedBy;
        this.updatedTimestamp = updatedTimestamp;
        this.createdTimestamp = createdTimestamp;
        this.isSensitive = isSensitive;
        this.anonymizationAlgorithm = anonymizationAlgorithm;
        this.tgtDatalakeName = tgtDatalakeName;
    }

    public long getId() {
        return this.id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getTgtTableName() {
        return this.tgtTableName;
    }

    public void setTgtTableName(String tgtTableName) {
        this.tgtTableName = tgtTableName.toLowerCase();
    }

    public String getTgtColumnName() {
        return this.tgtColumnName;
    }

    public void setTgtColumnName(String tgtColumnName) {
        this.tgtColumnName = tgtColumnName.toLowerCase();
    }

    public String getTgtColumnType() {
        return this.tgtColumnType;
    }

    /**
     * This property denotes the datatype of the target column, As the destination is Hive/Impala following are the valid
     * datatypes : string,integer,bigint,decimal,date,timestamp,boolean. This property should not contain null value.
     *
     * @param tgtColumnType
     */
    public void setTgtColumnType(String tgtColumnType) {
        this.tgtColumnType = tgtColumnType;
    }

    public Integer getTgtColumnLength() {
        return this.tgtColumnLength;
    }

    public void setTgtColumnLength(Integer tgtColumnLength) {
        this.tgtColumnLength = tgtColumnLength;
    }

    public Integer getTgtColumnPrecision() {
        return this.tgtColumnPrecision;
    }

    /**
     * This property is used when the tgtColumnType is decimal to specify the precision
     *
     * @param tgtColumnPrecision
     */
    public void setTgtColumnPrecision(Integer tgtColumnPrecision) {
        this.tgtColumnPrecision = tgtColumnPrecision;
    }

    public Boolean getTgtColumnPrimaryKey() {
        return this.tgtColumnPrimaryKey;
    }

    /**
     * This property should be true if the tgtColumnName is primary key.
     *
     * @param tgtColumnPrimaryKey
     */
    public void setTgtColumnPrimaryKey(Boolean tgtColumnPrimaryKey) {
        this.tgtColumnPrimaryKey = tgtColumnPrimaryKey;
    }

    public String getTgtColumnDescription() {
        return this.tgtColumnDescription;
    }

    public void setTgtColumnDescription(String tgtColumnDescription) {
        this.tgtColumnDescription = tgtColumnDescription;
    }

    public Boolean getIsForeignKey() {
        return isForeignKey;
    }

    public void setIsForeignKey(Boolean isForeignKey) {
        this.isForeignKey = isForeignKey;
    }

    public String getReferencedTableName() {
        return referencedTableName;
    }

    public void setReferencedTableName(String referencedTableName) {
        this.referencedTableName = referencedTableName;
    }

    /**
     * @return updated by value TODO Add appropriate codes during integrating to main source branch AUTO - if updated by
     *         MDD ETL Bootstrapping process MANUAL/NULL - if updated manually
     */
    public String getUpdatedBy() {
        return updatedBy;
    }

    /**
     * @see #getUpdatedBy()
     */
    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    /**
     * @return timestamp when this particular record is updated in the MDD Repository META_TABLE_MAPPING_INFO
     */
    public Date getUpdatedTimestamp() {
        return updatedTimestamp;
    }

    /**
     * @see #getUpdatedTimestamp()
     */
    public void setUpdatedTimestamp(Date updatedTimestamp) {
        this.updatedTimestamp = updatedTimestamp;
    }

    public Date getCreatedTimestamp() {
        return createdTimestamp;
    }

    public void setCreatedTimestamp(Date createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    public Boolean getIsSensitive() {
        return isSensitive;
    }

    public void setIsSensitive(Boolean isSensitive) {
        this.isSensitive = isSensitive;
    }

    public String getAnonymizationAlgorithm() {
        return anonymizationAlgorithm;
    }

    public void setAnonymizationAlgorithm(String anonymizationAlgorithm) {
        this.anonymizationAlgorithm = anonymizationAlgorithm;
    }

    public boolean semanticEquals(TargetColumn other) {
        if (other == null) {
            return false;
        }
        EqualsBuilder builder = new EqualsBuilder();
        builder.append(tgtTableName, other.tgtTableName);
        builder.append(tgtColumnName, other.tgtColumnName);
        builder.append(tgtDatalakeName, other.tgtDatalakeName);
        return builder.isEquals();
    }

    public boolean semanticEqualsIgnoreDatalakeName(TargetColumn other) {
        if (other == null) {
            return false;
        }
        EqualsBuilder builder = new EqualsBuilder();
        builder.append(tgtTableName, other.tgtTableName);
        builder.append(tgtColumnName, other.tgtColumnName);
        return builder.isEquals();
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, AbbreviatedToStringStyle.DEFAULT);
    }

}
