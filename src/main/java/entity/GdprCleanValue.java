package entity;

import java.sql.Timestamp;

/**
 * Java Bean for GDPR data.
 *
 * Represents a GDPR mapping (GDPR compliant value to it`s GDPR incompliant value), used during the ETL, to clean
 * incoming data off GDPR incompliant values.
 *
 */
public class GdprCleanValue {

    private long id;
    private String piiValue;
    private String collectorIds;
    private String updatedBy;
    private Timestamp createdTimestamp;
    private Timestamp lastUpdatedTimestamp;

    public GdprCleanValue() {
        this.collectorIds = "";
    }

    public GdprCleanValue(long id, String piiValue, String collectorIds) {
        this.id = id;
        this.piiValue = piiValue;
        this.collectorIds = collectorIds;
    }

    public long getId() {
        return id;
    }

    /**
     * The property id represents GDPR complaint value
     *
     * @param id
     */
    public void setId(long id) {
        this.id = id;
    }

    public String getCollectorIds() {
        return collectorIds;
    }

    /**
     * The property collectorIds represents comma-separated list of collectors, which populate this piiValue
     *
     * @param collectorIds
     */
    public void setCollectorIds(String collectorIds) {
        this.collectorIds = collectorIds;
    }

    public Timestamp getCreatedTimestamp() {
        return createdTimestamp;
    }

    /**
     * The property createdTimestamp represents time when the GDPR mapping was first generated
     *
     * @param createdTimestamp
     */
    public void setCreatedTimestamp(Timestamp createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    public Timestamp getLastUpdatedTimestamp() {
        return lastUpdatedTimestamp;
    }

    /**
     * Last time when data for the corresponding PII value was received (maps to the
     * last pa__arrival_ts in the VAC database).
     *
     * @param lastUpdatedTimestamp
     */
    public void setLastUpdatedTimestamp(Timestamp lastUpdatedTimestamp) {
        this.lastUpdatedTimestamp = lastUpdatedTimestamp;
    }

    public String getPiiValue() {
        return piiValue;
    }

    /**
     * The property piiValue represents GDPR incompliant value.
     *
     * @see <a href="https://en.wikipedia.org/wiki/Personally_identifiable_information">PII</a>
     * @param piiValue
     */
    public void setPiiValue(String piiValue) {
        this.piiValue = piiValue;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    /**
     * The property updatedBy is used for auditing purposes.
     *
     * E.g. user (mts-automation) has updated the record
     *
     * @param updatedBy
     */
    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

}
