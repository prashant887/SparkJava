package hcat;

import etl.PartitionColumn;
import etl.SystemColumn;
import org.jetbrains.annotations.NotNull;

public class ColumnSchema implements Comparable<ColumnSchema>{

    private final String name;
    private final String type;
    private final String comment;

    public ColumnSchema(String name, String type, String comment) {
        this.name = name;
        this.type = type;
        this.comment = comment;
    }

    public ColumnSchema(SystemColumn column) {
        this(column.getName(), column.getType(), column.getDescription());
    }

    public ColumnSchema(PartitionColumn column) {
        this(column.getName(), column.getType(), column.getDescription());
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public int compareTo(@NotNull ColumnSchema o) {
        return 0;
    }
}
