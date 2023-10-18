package util;

import api.Record;
import com.google.common.collect.ImmutableMap;
import etl.ExtractedMetrics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.parquet.column.ColumnDescriptor;

import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;

public class ExtractedMetricsWriteSupport extends WriteSupport<ExtractedMetrics> {

    private static final Logger log = LoggerFactory.getLogger(ExtractedMetricsWriteSupport.class);
    private transient volatile RecordConsumer recordConsumer;
    private final WriteContext writeContext;

    public ExtractedMetricsWriteSupport(MessageType messageType) {
        writeContext = new WriteContext(messageType, ImmutableMap.<String, String> of());
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return writeContext;
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(ExtractedMetrics em) {
        MessageType schema = writeContext.getSchema();
        recordConsumer.startMessage();
        int index = 0;
        Map<String, Object> metrics = em.getMetrics();

        for (ColumnDescriptor cd : schema.getColumns()) {
            String col = cd.getPath()[0];
            if (col.equalsIgnoreCase(Record.RECOMMENDED_PRIMARY_KEY)) {
                recordConsumer.startField(col, index);
                recordConsumer.addBinary(Binary.fromString(em.getTarget().getRowKey()));
                recordConsumer.endField(col, index);
            } else {
                if (metrics.containsKey(col)) {
                    Object o = metrics.get(col);
                    if (cd.getType().equals(PrimitiveType.PrimitiveTypeName.BINARY)) {
                        addBinary(em, o, col, index);
                    } else if (cd.getType().equals(PrimitiveType.PrimitiveTypeName.BOOLEAN)) {
                        addBoolean(em, o, col, index);
                    } else if (cd.getType().equals(PrimitiveType.PrimitiveTypeName.INT64)) {
                        addInt64(em, o, col, index);
                    } else if (cd.getType().equals(PrimitiveType.PrimitiveTypeName.INT96)) {
                        addInt96(em, o, col, index);
                    } else if (cd.getType().equals(PrimitiveType.PrimitiveTypeName.DOUBLE)) {
                        addDouble(em, o, col, index);
                    } else {
                        log.error(
                                "{}.{} is not handled! Primitive type name: {} , metric type: {} . This will be ignored in parquet file.",
                                em.getTarget().getTableName(), col, cd.getType(), o.getClass());
                    }
                } // else, column isn't in ExtractedMetrics
            }
            index++;
        }
        recordConsumer.endMessage();
    }
    private void addBinary(ExtractedMetrics em, Object o, String field, int index) {
        if (o instanceof String) {
            recordConsumer.startField(field, index);
            recordConsumer.addBinary(Binary.fromString((String) o));
            recordConsumer.endField(field, index);
        } else {
            log.error("{}.{} should be a string, but is of type {}. Will be ignored in parquet file.",
                    em.getTarget().getTableName(), field, o.getClass());
        }
    }

    private void addBoolean(ExtractedMetrics em, Object o, String field, int index) {
        if (o instanceof Boolean) {
            recordConsumer.startField(field, index);
            recordConsumer.addBoolean((Boolean) o);
            recordConsumer.endField(field, index);
        } else {
            log.error("{}.{} should be a boolean, but is of type {}. Will be ignored in parquet file.",
                    em.getTarget().getTableName(), field, o.getClass());
        }
    }

    private void addInt64(ExtractedMetrics em, Object o, String field, int index) {
        if (o instanceof Date) {
            Date date = (Date) o;
            recordConsumer.startField(field, index);
            recordConsumer.addLong(date.getTime());
            recordConsumer.endField(field, index);
        } else if (o instanceof Long || o instanceof Integer || o instanceof Short) {
            recordConsumer.startField(field, index);
            recordConsumer.addLong((Long) o);
            recordConsumer.endField(field, index);
        } else {
            log.error("{}.{} should be a Date or long, but is of type {}. Will be ignored in parquet file.",
                    em.getTarget().getTableName(), field, o.getClass());
        }
    }

    private void addInt96(ExtractedMetrics em, Object o, String field, int index) {
        if (o instanceof Date) {

            NanoTime nanoTime = NanoTimeUtils.getNanoTime(Timestamp.valueOf(o.toString()), false);
            recordConsumer.startField(field, index);
            nanoTime.writeValue(recordConsumer);
            recordConsumer.endField(field, index);
        } else {
            log.error("{}.{} should be a Date, but is of type {}. Will be ignored in parquet file.",
                    em.getTarget().getTableName(), field, o.getClass());
        }
    }

    private void addDouble(ExtractedMetrics em, Object o, String field, int index) {
        if (o instanceof Double || o instanceof Float) {
            recordConsumer.startField(field, index);
            recordConsumer.addDouble((Double) o);
            recordConsumer.endField(field, index);
        } else {
            log.error("{}.{} should be a double, but is of type {}. Will be ignored in parquet file.",
                    em.getTarget().getTableName(), field, o.getClass());
        }
    }

}
