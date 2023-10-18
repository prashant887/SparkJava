package etl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The partitions columns used in history table.
 * <p>
 * Partitioning is a technique for physically dividing the data during loading, based on values from one or more
 * columns, to speed up queries that test those columns.
 *
 * <p>
 * The order of the columns in the enum is also the order in which are created in the database.
 *
 *  http://www.cloudera.com/documentation/enterprise/latest/topics/impala_partitioning.html
 */
public enum PartitionColumn {

    /**
     * @see SystemColumn#PA__ARRIVAL_DAY
     */
    PA__ARRIVAL_DAY(SystemColumn.PA__ARRIVAL_DAY.getType(), SystemColumn.PA__ARRIVAL_DAY.getDescription()),

    /**
     * @see SystemColumn#PA__COLLECTOR_ID
     */
    PA__COLLECTOR_ID(SystemColumn.PA__COLLECTOR_ID.getType(), SystemColumn.PA__COLLECTOR_ID.getDescription()),

    /**
     * @see SystemColumn#PA__SCHEMA_VERSION
     */
    PA__SCHEMA_VERSION(SystemColumn.PA__SCHEMA_VERSION.getType(), SystemColumn.PA__SCHEMA_VERSION.getDescription());

    private final String type;
    private final String description;

    private PartitionColumn(String type, String description) {
        this.type = type;
        this.description = description;
    }

    public String getName() {
        return name().toLowerCase();
    }

    public String getType() {
        return this.type;
    }

    public String getDescription() {
        return this.description;
    }

    private static final List<String> NAMES = new ArrayList<>();

    static {
        for (PartitionColumn partitionColumn : PartitionColumn.values()) {
            NAMES.add(partitionColumn.getName());
        }
    }

    /**
     * @return collection of lowercase names
     */
    public static Collection<String> names() {
        return Collections.unmodifiableList(NAMES);
    }

}
