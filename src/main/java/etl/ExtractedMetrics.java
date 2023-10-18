package etl;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.CaseInsensitiveLinkedMap;

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class ExtractedMetrics implements Serializable {

    private static final long serialVersionUID = -2743670146590796197L;
    private final Map<String /*column name*/, Object> metrics = new CaseInsensitiveLinkedMap<>();
    private final Map<String /*referenced table*/, ExtractedMetricsRelation> relations = new CaseInsensitiveLinkedMap<>();
    private String groupName;

    private final TableLocation target;

    private static Logger log = LoggerFactory.getLogger(ExtractedMetrics.class);

    public ExtractedMetrics(String groupName, TableLocation location) {
        Validate.notNull(groupName);
        Validate.notNull(location);
        this.groupName = groupName;
        this.target = location;
    }

    public ExtractedMetrics(TableLocation location, Map<String, Object> metrics) {
        this(makeGroupName(location.getTableName(), location.getRowKey()), location, metrics);
    }

    public ExtractedMetrics(TableLocation location, Map<String, Object> metrics,
                            Collection<ExtractedMetricsRelation> relations) {
        this(makeGroupName(location.getTableName(), location.getRowKey()), location, metrics, relations);
    }

    public ExtractedMetrics(String groupName, TableLocation location, Map<String, Object> metrics) {
        this(groupName, location);
        Validate.notNull(metrics);
        this.metrics.putAll(metrics);
    }

    public ExtractedMetrics(String groupName, TableLocation location, Map<String, Object> metrics,
                            Collection<ExtractedMetricsRelation> relations) {
        this(groupName, location, metrics);
        this.addRelations(relations);
    }

    public ExtractedMetrics(ExtractedMetrics extractedMetrics) {
        this(extractedMetrics.getGroupName(), extractedMetrics.getTarget(), extractedMetrics.getMetrics(),
                extractedMetrics.getRelations());
    }

    public static String makeGroupName(String typeName, String localId) {
        return typeName + '|' + localId;
    }

    public static String makeGroupName(String localId) {
        return localId;
    }

    /**
     * Get dictionary of extracted metrics by this class(?)
     *
     * @param
     *            of metrics where the key is the dimension/property/column and the value is the value of the
     *           metric, required
     */
    public Map<String, Object> getMetrics() {
        return this.metrics;
    }

    public Object get(String key) {
        return this.metrics.get(key);
    }

    /**
     * @see
     */
    public TableLocation getTarget() {
        return this.target;
    }

    public long getArrivalTsBundleTime() {
        return getTime(SystemColumn.PA__ARRIVAL_TS.getName());
    }

    public long getProcessedTsTime() {
        return getTime(SystemColumn.PA__PROCESSED_TS.getName());
    }

    private long getTime(String columnName) {
        Object ts = this.metrics.get(columnName);
        if (ts == null) {
            log.error("Missing {} system column in row for table {}", columnName, target.getTableName());
        }
        long time;
        if (ts instanceof Date) {
            time = ((Date) ts).getTime();
        } else {
            time = ts != null ? (long) ts : -1;
        }
        return time;
    }

    /**
     * @return the relations of this extracted metrics
     */
    public Collection<ExtractedMetricsRelation> getRelations() {
        return this.relations.values();
    }

    /**
     * add multiple relations in one go.
     *
     * @see {@link #addRelation(ExtractedMetricsRelation)}
     */
    public void addRelations(Collection<ExtractedMetricsRelation> relations) {
        for (ExtractedMetricsRelation r : relations) {
            this.addRelation(r);
        }
    }

    /**
     * Get relation for this table or null if it does not exist.
     */
    public ExtractedMetricsRelation getRelation(final String tableName) {
        return this.relations.get(tableName);
    }

    /**
     *
     * If a relation for this table already exists and it includes modifying the primary key of the entity (appendKey or
     * prependKey flag is set to true), nothing is done. Otherwise, the relation is updated.
     */
    public void addRelation(ExtractedMetricsRelation relation) {
        ExtractedMetricsRelation existingRelation = this.relations.get(relation.getReferencesTable());

        if (existingRelation == null || ExtractedMetricsRelation.PrimaryKeyTransformation.NONE.equals(existingRelation.getKeyTransformation())) {
            this.relations.put(relation.getReferencesTable(), relation);
        } else {
            log.debug("A relation to {} table already exists and it includes modifying the primary key of the entity. "
                    + "Will not override it. Existing relation: {}", relation.getReferencesTable(), relation);
        }
    }

    /**
     * Gets the logical local group name for these metrics. For example metrics for the same product would have the same
     * product name and id as group name It can be used as map key for a mapper part of mapreduce job.
     */
    public String getGroupName() {
        return this.groupName;
    }

    public void setGroup(String groupName) {
        this.groupName = groupName;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

}
