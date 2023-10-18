package api;

import java.util.Date;
import java.util.Objects;

/**
 * Wraps one value of either String, Double, Integer, Long, Boolean or Date. Values cannot be <code>null</code>.
 * AnyPrimitive is used instead of Object in order to restrict the value type.
 *
 * @see RelatedRecord if you need a wrapper of another record
 */
public class AnyPrimitive {

    private final Object value;

    public AnyPrimitive(Integer value) {
        System.out.println("AnyPrimitive "+value);
        this.value = value;
    }

    public AnyPrimitive(Long value) {
        this.value = value;
    }

    public AnyPrimitive(Double value) {
        this.value = value;
    }

    public AnyPrimitive(String value) {
        this.value = value;
    }

    public AnyPrimitive(Boolean value) {
        this.value = value;
    }

    public AnyPrimitive(Date value) {
        this.value = value;
    }

    public Object getValue() {
        return this.value;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        } else {
            return Objects.equals(value, ((AnyPrimitive) obj).value);
        }
    }

    @Override
    public String toString() {
        return "AnyPrimitive [value=" + value + ", type=" + (value == null ? null : value.getClass().getSimpleName()) + ']';
    }
}
