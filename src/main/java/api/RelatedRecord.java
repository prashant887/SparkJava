package api;

import java.util.Objects;

public class RelatedRecord implements RecordKey {

    private final String id;
    private final String type;

    public RelatedRecord(String id) {
        this(id, null);
    }

    public RelatedRecord(String id, String type) {
        this.id = id;
        this.type = type;
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public String getType() {
        return this.type;
    }

    @Override
    public int hashCode() {
        return 31 * (31 + ((id == null) ? 0 : id.hashCode())) + ((type == null) ? 0 : type.hashCode());
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (getClass() != obj.getClass()) {
            return false;
        } else {
            final RelatedRecord other = (RelatedRecord) obj;
            return Objects.equals(id, other.id) && Objects.equals(type, other.type);
        }
    }

    @Override
    public String toString() {
        return "RelatedRecord [id=" + id + ", type=" + type + ']';
    }
}
