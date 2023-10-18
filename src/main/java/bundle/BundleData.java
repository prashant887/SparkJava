package bundle;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Date;

/**
 * Represents a Bundle that can be processed by the analytics system.
 */
public class BundleData {

    public static final class ColumnName {
        // must be upper-cased
        public final static String INTERNAL_ID = "INTERNAL_ID";
        public final static String SIZE_IN_BYTES = "SIZE_IN_BYTES";
        public final static String UNCOMPRESSED_IN_BYTES = "UNCOMPRESSED_IN_BYTES";
        public final static String KAFKA_PARTITION = "PA__KAFKA_PARTITION";
        public final static String KAFKA_PARTITION_OFFSET = "PA__KAFKA_PARTITION_OFFSET";
        public final static String COLLECTION__FK = "COLLECTION__FK";
        public final static String ENVELOPE_TS = "ENVELOPE_TS";
        public final static String CLIENT_IP_PATH = "PA__CLIENT_IP_PATH";
        public final static String NUM_ROWS = "num_rows";
        public final static String NUM_ERRORS = "num_errors";

    }

    private String id;

    private Boolean isExternal;
    private String collectorResourceUri;
    private String collectorIdType;
    private String collectorInstanceId;
    private Date downloadDate;
    private Long internalId;
    private Long sizeInBytes;
    private String clientIpPath;

    private boolean lastChunk = true;//default true for single chunk bundles
    private String collectionFK;
    private String kafkaPartition;
    private String kafkaPartitionOffset;

    public BundleData() {
    }

    public BundleData(String id) {
        this.id = id;
    }

    /**
     * @return Unique identification for bundles, correspond to the file name of the bundle
     * @see com.vmware.ph.BundleData.model.Bundle#getFileName()
     */
    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return return the bundle is from external source (collector)
     * @see com.vmware.ph.model.upload.UploadRequest#getIsClientIpInternal()
     */
    public Boolean getIsExternal() {
        return this.isExternal;
    }

    public void setIsExternal(Boolean isExternal) {
        this.isExternal = isExternal;
    }


    /**
     * @return collector resource url.
     * @see com.vmware.ph.client.model.Collector#getResourceUrl()
     */
    public String getCollectorResourceUri() {
        return this.collectorResourceUri;
    }

    public void setCollectorResourceUri(String collectorResourceUri) {
        this.collectorResourceUri = collectorResourceUri;
    }

    /**
     * @see com.vmware.ph.client.model.Collector#getCollectorId()
     */
    public String getCollectorIdType() {
        return this.collectorIdType;
    }

    public void setCollectorIdType(String collectorIdType) {
        this.collectorIdType = collectorIdType;
    }

    /**
     * @see com.vmware.ph.BundleData.model.Bundle#getRecieveDate()
     */
    public Date getDownloadDate() {
        return this.downloadDate;
    }

    public void setDownloadDate(Date downloadDate) {
        this.downloadDate = downloadDate;
    }

    /**
     * @return collector instance id
     * @see com.vmware.ph.client.api.commondataformat.dimensions.Collector.getCollectorInstanceId()
     */
    public String getCollectorInstanceId() {
        return this.collectorInstanceId;
    }

    public void setCollectorInstanceId(String collectorInstanceId) {
        this.collectorInstanceId = collectorInstanceId;
    }

    /**
     * @see com.vmware.ph.BundleData.model.Bundle#getInternalId()
     */
    public Long getInternalId() {
        return this.internalId;
    }

    public void setInternalId(Long internalId) {
        this.internalId = internalId;
    }

    /**
     * @return the bundle size in bytes
     * @see com.vmware.ph.client.model.Bundle.getFileSize()
     */
    public Long getSizeInBytes() {
        return this.sizeInBytes;
    }

    public void setSizeInBytes(Long sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }


    /**
     * @return comma-separated path of IPv4 or v6 addresses through which the client request to the REST servers has
     *         passed, as reported by intermediate forward and reverse proxies; may be null or empty if data was not
     *         available or restricted.
     */
    public String getClientIpPath() {
        return clientIpPath;
    }

    public void setClientIpPath(String clientIpPath) {
        this.clientIpPath = clientIpPath;
    }

    public boolean isLastChunk() {
        return lastChunk;
    }

    public void setLastChunk(boolean lastChunk) {
        this.lastChunk = lastChunk;
    }

    public String getCollectionFK() {
        return collectionFK;
    }

    public void setCollectionFK(String collectionFK) {
        this.collectionFK = collectionFK;
    }

    public String getKafkaPartition() {
        return kafkaPartition;
    }

    public void setKafkaPartition(String kafkaPartition) {
        this.kafkaPartition = kafkaPartition;
    }

    public String getKafkaPartitionOffset() {
        return kafkaPartitionOffset;
    }

    public void setKafkaPartitionOffset(String kafkaPartitionOffset) {
        this.kafkaPartitionOffset = kafkaPartitionOffset;
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
