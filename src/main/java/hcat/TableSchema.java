package hcat;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import util.CaseInsensitiveLinkedMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Mutable description of a table schema
 *
 * <p>
 * At any point, it is guaranteed that the schema is valid and, specifically there are no two columns with the same
 * name, even regardless of case. It is assumed that is immutable.
 *
 * <p>
 * Column names are case-sensitive for {@link #equals(Object)} and {@link #hashCode()} for keep compatibility with
 * database, which preserve input case, but still require case-insensitive uniqueness.
 * <p>
 * Not thread safe.
 */
public class TableSchema {

    private String database;
    private String table;
    private String comment;
    private boolean isEvent; // TODO: this should be isView, this class has not concept of event tables

    private final Map<String, ColumnSchema> columnsMap = new CaseInsensitiveLinkedMap<>();
    private final Map<String, ColumnSchema> partitionsMap = new CaseInsensitiveLinkedMap<>();

    public TableSchema(String table) {
        Validate.notEmpty(table);
        this.table = table;
    }

    public TableSchema(TableSchema tableSchema) {
        Validate.notNull(tableSchema, "Trying to copy TableSchema which is null.");
        this.database = tableSchema.getDatabase();
        this.table = tableSchema.getTable();
        this.isEvent = tableSchema.isEvent();
        this.comment = tableSchema.getComment();
        setColumns(tableSchema.getColumns());
        setPartitions(tableSchema.getPartitions());
    }

    /**
     * @return the database. If null or empty, the caller should assume that the "default" database is specified.
     */
    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    /**
     * @return the table name, not null or empty
     */
    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        Validate.notEmpty(table);
        this.table = table;
    }

    /**
     * @return may be null or empty
     */
    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    /**
     * @return a mutable copy of the columns in the schema, in order. The result doesn't include partition columns.
     *
     * <p>No columns have the same name, even ignoring case.
     */
    public List<ColumnSchema> getColumns() {
        // values() doesn't implement neither hashcode or equals, so we copy it into ArrayList that does
        return new ArrayList<>(columnsMap.values());
    }

    /**
     * Replaces the columns in the schema with the provided ones, maintaining iteration order
     *
     * <p>
     * If multiple input columns have the same name, ignoring case, only the first one will be kept
     *
     * @param columns required
     */
    public void setColumns(Collection<ColumnSchema> columns) {
        this.columnsMap.clear();

        for (ColumnSchema col : columns) {
            addColumn(col);
        }
    }

    /**
     * Adds the column if the name is unique, ignoring case
     *
     * @param col
     *           required
     * @return if the columns was added
     */
    public boolean addColumn(ColumnSchema col) {
        Validate.notNull(col);
        return addColumnsInternal(this.columnsMap, col);
    }

    /**
     * Removes a column from the table schema (the search is case insensitive) and returns the column.
     * If not found returns null.
     */
    public ColumnSchema removeColumn(String name) {
        return columnsMap.remove(name);
    }

    /**
     * Search for a column in the table schema (it's case insensitive). If not found returns null.
     */
    public ColumnSchema findColumn(String name) {
        return columnsMap.get(name);
    }

    /**
     * Search for a partition in the table schema (it's case insensitive). If not found returns null.
     */
    public ColumnSchema findPartition(String name) {
        return partitionsMap.get(name);
    }

    public ColumnSchema findColumnOrPartition(String name) {
        ColumnSchema column = findPartition(name);
        if (column == null) {
            column = findColumn(name);
        }
        return column;
    }
    /**
     * @return a mutable copy of the partitions in the schema, in order
     *
     * <p>No partitions have the same name, even ignoring case.
     */
    public List<ColumnSchema> getPartitions() {
        // values() doesn't implement neither hashcode or equals, so we copy it into ArrayList that does
        return new ArrayList<>(partitionsMap.values());
    }

    /**
     * Replaces the partitions in the schema with the provided ones, maintaining iteration order
     *
     * <p>
     * If multiple input partitions have the same name, ignoring case, only the first one will be kept
     *
     * @param columns required
     */
    public void setPartitions(Collection<ColumnSchema> columns) {
        Validate.notNull(columns);
        this.partitionsMap.clear();

        for (ColumnSchema col : columns) {
            addPartition(col);
        }
    }

    /**
     * Adds the partitions if the name is unique, ignoring case
     *
     * @param col
     *           required
     * @return if the columns was added
     */
    public boolean addPartition(ColumnSchema col) {
        Validate.notNull(col);
        return addColumnsInternal(this.partitionsMap, col);
    }

    private static boolean addColumnsInternal(Map<String, ColumnSchema> map, ColumnSchema col) {
        String key = col.getName();
        boolean willAdd = !map.containsKey(key);
        if (willAdd) {
            map.put(key, col);
        }
        return willAdd;
    }

    /**
     * @return the qualified name of the table
     * <p>
     * This is the database and table name in the format "database.table"
     */
    public String getQualifiedName() {
        return String.format("%s.%s", this.database, this.table);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    public String toStringFormatted() {
        return String.format("%s.%s:\n" + "==Columns==\n%s\n" + "==Partitions==\n%s\n",
                database, table,
                StringUtils.join(columnsMap.values(), "\n"),
                StringUtils.join(partitionsMap.values(), "\n"));
    }
    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    public boolean isEvent() {
        return isEvent;
    }

    public void setIsEvent(boolean isEvent) {
        this.isEvent = isEvent;
    }




}
