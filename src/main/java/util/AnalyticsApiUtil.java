package util;
import api.AnyPrimitive;

import java.util.Date;

public class AnalyticsApiUtil {
    private static final AnyPrimitive EMPTY_STRING = new AnyPrimitive("");
    private static final AnyPrimitive LONG_0 = new AnyPrimitive(0L);
    private static final AnyPrimitive DOUBLE_0 = new AnyPrimitive(0.0);
    private static final AnyPrimitive TRUE = new AnyPrimitive(true);
    private static final AnyPrimitive FALSE = new AnyPrimitive(false);

    public static AnyPrimitive toAnyPrimitive(Object value) {
        System.out.println(" \n toAnyPrimitive "+value);
        if (value == null) {
            return null;
        } else if (value instanceof Integer) {
            return new AnyPrimitive((Integer) value);
        } else if (value instanceof Long) {
            if ((Long) value == 0L) {
                // trace("LONG_0");
                return LONG_0;
            }
            return new AnyPrimitive((Long) value);
        } else if (value instanceof Double) {
            if ((Double) value == 0) {
                // trace("DOUBLE_0");
                return DOUBLE_0;
            }
            return new AnyPrimitive((Double) value);
        } else if (value instanceof Boolean) {
            return (Boolean) value ? TRUE : FALSE;
        } else if (value instanceof String) {
            if ("".equals(value)) {
                // trace("EMPTY_STRING");
                return EMPTY_STRING;
            }
            return new AnyPrimitive((String) value);
        } else if (value instanceof Date) {
            return new AnyPrimitive((Date) value);
        } else {
            throw new IllegalArgumentException(
                    value + " must be Int, Long, Double, Boolean, String or Date but is "
                            + value.getClass());
        }

    }
    public static AnyPrimitive toAnyPrimitiveAdjustNumberType(Object value) {
        System.out.println("\n toAnyPrimitiveAdjustNumberType "+value+" Type "+value.getClass());
        if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            value = ((Number) value).longValue();
        } else if (value instanceof Float) {
            value = ((Number) value).doubleValue();
        } else if (value.equals(null)) {
            // JSONObject.get() returns JSONObject$Null for null values.
            return toAnyPrimitive(null);
        }
        return toAnyPrimitive(value);
    }

}
