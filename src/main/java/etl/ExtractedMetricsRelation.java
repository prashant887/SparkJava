package etl;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Shows a relation to a different entity whose primary key is unknown at the current stage of ETL. The entity is
 * defined by its type and the fact that it's a singleton, i.e. there is a singleton entity of this type in the dataset.
 *
 * Describes one direction of the relation (this -> referencesTable)
 */

public class ExtractedMetricsRelation {
    /**
     * Describes if and how the referenced entity should affect the primary key of the current {@link ExtractedMetrics}.
     *
     * This is useful in case of 'weak entities' which cannot be uniquely identified by their own key. In order to create
     * a primary key, they also need a foreign key to another entity. A typical example is a ProductLineItem which
     * doesn't make sense without its parent Order.
     *
     * The primary key of the referenced entity can be added in the beginning (PREPEND_KEY) or in the end (APPEND_KEY) of
     * current key.
     */
    public static enum PrimaryKeyTransformation {
        NONE, PREPEND_KEY, APPEND_KEY;
    }

    private String referencesTable;
    private PrimaryKeyTransformation keyTransformation;
    private String foreignKeyName;

    /**
     * @param referencesTable,
     *           the foreign table that is referenced
     * @param keyTransformation,
     *           whether the foreign primary key should be added to the current primary key
     */
    public ExtractedMetricsRelation(String referencesTable,
                                    PrimaryKeyTransformation keyTransformation, String foreignKeyName) {
        this.referencesTable = referencesTable != null ? referencesTable.toLowerCase() : referencesTable;
        this.keyTransformation = keyTransformation;
        this.foreignKeyName = foreignKeyName;
    }

    public ExtractedMetricsRelation(String referencesTable,String foreignKeyName) {
        this(referencesTable, PrimaryKeyTransformation.NONE, foreignKeyName);
    }

    public ExtractedMetricsRelation() {
        this(null, null);
    }

    public final String getReferencesTable() {
        return this.referencesTable;
    }

    public final PrimaryKeyTransformation getKeyTransformation() {
        return this.keyTransformation;
    }

    public void setReferencesTable(String referencesTable) {
        this.referencesTable = referencesTable;
    }

    public void setKeyTransformation(PrimaryKeyTransformation keyTransformation) {
        this.keyTransformation = keyTransformation;
    }

    public String getForeignKeyName() {
        return this.foreignKeyName;
    }

    public void setForeignKeyName(String foreignKeyName) {
        this.foreignKeyName = foreignKeyName;
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
