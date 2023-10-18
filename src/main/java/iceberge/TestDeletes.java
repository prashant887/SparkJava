package iceberge;

import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class TestDeletes {


    private static final Schema ROW_SCHEMA=
            new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.required(2, "name", Types.StringType.get()),
                    Types.NestedField.optional(3, "description", Types.StringType.get())
                    );
    /*
    private static final CloseableIterable<StructLike> ROWS =
            CloseableIterable.withNoopClose(
                    Lists.newArrayList(
                            Row.of(0L, "a", "panda"),
                            Row.of(1L, "b", "koala"),
                            Row.of(2L, "c", new Utf8("kodiak")),
                            Row.of(4L, new Utf8("d"), "gummy"),
                            Row.of(5L, "e", "brown"),
                            Row.of(6L, "f", new Utf8("teddy")),
                            Row.of(7L, "g", "grizzly"),
                            Row.of(8L, "h", null)));

     */
    public static void main(String[] args) {

    }
}
