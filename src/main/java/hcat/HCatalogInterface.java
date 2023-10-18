package hcat;

public interface HCatalogInterface extends AutoCloseable {
    /**
     * Result of an updateTableSchema operation
     *
     * <p>
     * finalSchema can be different from source schema even is changeMade is false. The actual schema in the database was
     * already a superset of source schema so an update was not necessary.
     */
    public class UpdateResult {

        /**
         * The schema in the database after the update
         *
         * <p>It may not be the same as the input schema if the table already exists.
         */
        public final TableSchema finalSchema;

        /**
         * If a change in the database schema had to be made
         */
        public final boolean changeMade;

        public UpdateResult(TableSchema finalSchema, boolean changeMade) {
            this.finalSchema = finalSchema;
            this.changeMade = changeMade;
        }
    }

    /**
     * Updates the schema of the target table name with the given source schema if needed.
     *
     * <p>
     * If the table does not exists it will create it. Avoid issuing a command to change the schema if it is not needed.
     *
     * <p><b>NOTE:</b> New partition definitions cannot be created if the table already exits (and is not being created by
     * this method)
     *
     *
     */
    UpdateResult updateTableSchema(TableSchema tableSchema) throws HCatalogException;

}
