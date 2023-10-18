package api;

public interface RecordKey {


    /**
     * @return the type of the record. For example type could be a table name in relational model.
     */
    String getType();

    /**
     * @return primary key of the record, which identifies it uniquely within the context
     */
    String getId();
}
