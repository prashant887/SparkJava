package api;


import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Composite data structure composed of different extracted metrics.
 * <p>
 *
 * A record is an entity defined by a type and list of key value pairs (metrics). In relational model one record could
 * represent one row in a table.Any further transformations and schema(primary key, foreign keys, validations) are
 * defined in the Metadata Repository and applied automatically by the Analytics system.
 * <p>
 * If the primary key is known while parsing, it is recommended to add to the output records as key
 */
public class Record implements RecordKey{

    private final String type; //This is for @ID
    private final Map<String, AnyPrimitive> metrics;
    private final Map<String, RelatedRecord> relatedRecords;

    /**
     * The recommended key for storing primary key in <code>metrics</code> map.
     *
     * This is the name of the column that is used by the Analytics platform as primary key by default. In order to use
     * another column, product teams should specify this in the Metadata Repository.
     */
    public static final String RECOMMENDED_PRIMARY_KEY = "ID";

    /**
     * @param type
     *           required, the type of the record. See also {@link #getType()}
     * @param metrics
     *           required, list of key value pairs that are the metrics for this record. See also {@link #getMetrics()}.
     *           If the order of metrics is important, we recommend that a {@link LinkedHashMap} instance is provided.
     */
    public Record(String type, Map<String, AnyPrimitive> metrics) {
        this(type, metrics, Collections.<String,RelatedRecord>emptyMap());
    }

    /**
     * @param type
     *           required, the type of the record. See also {@link #getType()}
     * @param metrics
     *           required, list of key value pairs that are the metrics for this record. See also {@link #getMetrics()}.
     *           If the order of metrics is important, we recommend that a {@link LinkedHashMap} instance is provided.
     *           Either way if the same file is parsed twice the order of the extracted metrics should be the same. The
     *           input map is not copied to support different derived maps like function maps or composite maps. It's up
     *           to the caller to ensure that the input is not modified after the record is created.
     * @param relatedRecords
     *           list of key value pairs that are the relations of this record to other records. See also
     *           {@link #getRelatedRecords()}. If the order of metrics is important, we recommend that a
     *           {@link LinkedHashMap} instance is provided. Either way if the same file is parsed twice the order of the
     *           extracted metrics should be the same.
     */
    public Record(String type, Map<String, AnyPrimitive> metrics, Map<String, RelatedRecord> relatedRecords) {
        ValidationUtils.validateNonBlank(type, "The type must not be blank");
        ValidationUtils.validateNotNull(metrics, "Metrics must be specified. Cannot be null");
        ValidationUtils.validateNotNull(relatedRecords, "Related records must be specified. Cannot be null");
        this.type = type;
        this.metrics = Collections.unmodifiableMap(metrics);
        this.relatedRecords = Collections.unmodifiableMap(relatedRecords);
    }

    /**
     * Get dictionary of extracted metrics for this record
     *
     * @return dictionary of metrics where the key is the dimension/property/column and the value is the value of the
     *         metric. The value of each key value pair should be either {@link AnyPrimitive} or {@code null}.
     */
    public Map<String, AnyPrimitive> getMetrics() {
        return this.metrics;
    }

    /**
     * Get dictionary of extracted relations for this record
     *
     * @return dictionary of related records where the key is the name of the relation/property/column and the value is
     *         the identifier of the related record. The value of each key value pair cannot be null.
     */
    public Map<String, RelatedRecord> getRelatedRecords() {
        return this.relatedRecords;
    }

    /**
     * Shortcut method that returns the corresponding value for the passed key in this record metrics.
     *
     * @param key required, the key of the metric
     * @return the value corresponding to the given key
     */
    public Object get(String key) {
        AnyPrimitive object = this.metrics.get(key);
        return object != null ? object.getValue() : null;
    }

    @Override
    public String getId() {
        Object id = get(RECOMMENDED_PRIMARY_KEY);
        return id != null ? id.toString() : null;
    }

    @Override
    public String getType() {
        return this.type;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((metrics == null) ? 0 : metrics.hashCode());
        result = prime * result + ((relatedRecords == null) ? 0 : relatedRecords.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Record other = (Record) obj;
        if (metrics == null) {
            if (other.metrics != null)
                return false;
        } else if (!metrics.equals(other.metrics))
            return false;
        if (relatedRecords == null) {
            if (other.relatedRecords != null)
                return false;
        } else if (!relatedRecords.equals(other.relatedRecords))
            return false;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Record [type=" + type + ", metrics=" + metrics + ", relations=" + relatedRecords + "]";
    }


}
