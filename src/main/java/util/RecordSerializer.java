package util;

import api.AnyPrimitive;
import api.Record;
import api.RelatedRecord;
import com.google.gson.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import com.google.gson.JsonPrimitive;

import java.lang.reflect.Type;
import java.util.Map;

public class RecordSerializer implements JsonSerializer<Record> {

    public static final Integer MAX_STRING_LENGTH = 5000;

    @Override
    public JsonElement serialize(Record record, Type type, JsonSerializationContext context) {
        JsonObject result = new JsonObject();
        result.add("table", new JsonPrimitive(record.getType()));

        JsonArray recordAttributes = new JsonArray();

        Map<String, AnyPrimitive> metrics = record.getMetrics();

        for(Map.Entry<String, AnyPrimitive> entry: metrics.entrySet()) {
            // ignore null values. Wait for a non-null value, which might give us type information.
            if (entry.getValue() != null && entry.getValue().getValue() != null) {
                JsonObject obj = new JsonObject();
                obj.add("name", new JsonPrimitive(entry.getKey().toString()));
                String primType = getPrimitiveType(entry.getValue());
                String primVal = entry.getValue().getValue().toString();
                if (primType.equals(TypeInfoFactory.stringTypeInfo.getTypeName())) {
                    primVal = StringUtils.abbreviate(primVal, MAX_STRING_LENGTH);
                }

                obj.add("value", new JsonPrimitive(primVal));
                obj.add("type", new JsonPrimitive(primType));
                recordAttributes.add(obj);

            }

            }
        Map<String, RelatedRecord> relatedRecords = record.getRelatedRecords();
        for(Map.Entry<String, RelatedRecord> entry: relatedRecords.entrySet()) {
            if (entry.getValue() != null) {
                JsonObject obj = new JsonObject();
                obj.add("name",new JsonPrimitive(entry.getKey().toString()));
                obj.add("value", new JsonPrimitive(entry.getValue().getId()));
                obj.add("type", new JsonPrimitive("string"));
                obj.add("is_foreign_key", new JsonPrimitive(true));
                if (entry.getValue().getType() != null) {
                    obj.add("referenced_table_name", new JsonPrimitive(entry.getValue().getType()));
                }
                recordAttributes.add(obj);
            }
        }
        result.add("columns", recordAttributes);

        return result;
        }
    private static String getPrimitiveType(AnyPrimitive value) {
        PrimitiveTypeInfo type;
        String tp = value.getValue() != null ? value.getValue().getClass().getSimpleName() : String.class.getSimpleName();
        switch (tp) {
            case "Integer":
            case "Long":
                type = TypeInfoFactory.longTypeInfo;
                break;
            case "Double":
                type = TypeInfoFactory.doubleTypeInfo;
                break;
            case "Boolean":
                type = TypeInfoFactory.booleanTypeInfo;
                break;
            case "Date":
                type = TypeInfoFactory.timestampTypeInfo;
                break;
            case "String":
            default:
                type = TypeInfoFactory.stringTypeInfo;
                break;
        }
        return type.getTypeName();
    }
}
