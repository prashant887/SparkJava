package etl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Columns that are automatically added for all tables - bundle, collector, timestamp, etc.
 */
// System columns should start with 'PA__' prefix in order to be easily distinguished from custom columns.
// This is mandatory for new columns, although it is not yet true for the existing columns.
public enum SystemColumn {

    /**
     * Foreign key to the bundle (or upload) that contributed the given row.
     * <p>
     * In history database each row will be populated from a single bundle.
     */
    PA__BUNDLE__FK("string", "Foreign key to the bundle that contributed the given row"),

    /**
     * The collector ID of the bundle referenced by {@link #}.
     */
    PA__COLLECTOR_INSTANCE_ID("string", "Collector instance ID"),

    /**
     * Whether the bundle referenced in {@link #PA__BUNDLE__FK} is external, i.e. it came from outside VMware network.
     */
    PA__IS_EXTERNAL("boolean",
            "Whether the bundle referenced in PA__BUNDLE__FK is external, i.e. it came from outside VMware network."),

    /**
     * Timestamp when this row arrived at VMware. <br/>
     * This is the primary column recommended for trending reports on top of history database.
     */
    PA__ARRIVAL_TS("timestamp", "Timestamp when this row arrived at VMware"),

    /**
     * Timestamp when this row was processed/last updated by Product Analytics platform
     */
    PA__PROCESSED_TS("timestamp", "Timestamp when this row was processed/last updated by Product Analytics platform"),


    /**
     * {@link SystemColumn#PA__ARRIVAL_TS} truncated to day. <br/>
     * The column is also used for partitioning in 'history' database - see PartitionColumn#PA__ARRIVAL_DAY
     */
    PA__ARRIVAL_DAY("bigint", "Unix timestamp when this row arrived at VMware, truncated to day"),

    /**
     * The collector ID of the bundle referenced by {@link }. <br/>
     * The column is also used for partitioning in 'history' database - see PartitionColumn#PA__COLLECTOR_ID
     */
    PA__COLLECTOR_ID("string", "ID of the collector that contributed to the given row"),

    /**
     * Schema version. Reserved for future use. <br/>
     * The column is also used for partitioning in 'history' database - see PartitionColumn#PA__SCHEMA_VERSION
     */
    PA__SCHEMA_VERSION("bigint", "Schema version. Reserved for future use");

    private final String type;
    private final String description;

    private SystemColumn(String type, String description) {
        this.type = type;
        this.description = description;
    }

    public String getName() {
        return name().toLowerCase().intern();
    }

    public String getType() {
        return this.type;
    }

    public String getDescription() {
        return this.description;
    }

    private static final List<String> NAMES = new ArrayList<>();

    static {
        for (SystemColumn systemColumn : values()) {
            NAMES.add(systemColumn.getName());
        }
    }

    public static Collection<String> names() {
        return Collections.unmodifiableList(NAMES);
    }

}
