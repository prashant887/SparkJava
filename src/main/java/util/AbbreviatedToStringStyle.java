package util;


import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringStyle;

public class AbbreviatedToStringStyle extends ToStringStyle {

    public static final AbbreviatedToStringStyle DEFAULT = new AbbreviatedToStringStyle(100);

    private static final long serialVersionUID = -4178039339351863245L;

    private final int maxLength;

    public AbbreviatedToStringStyle(int maxLength) {
        super();

        // Match behavior of ToStringStyle#SHORT_PREFIX_STYLE
        this.setUseShortClassName(true);
        this.setUseIdentityHashCode(false);

        this.maxLength = maxLength;
    }

    @Override
    protected void appendDetail(StringBuffer buffer, String fieldName, Object value) {
        if (value instanceof CharSequence) {
            super.appendDetail(buffer, fieldName, StringUtils.abbreviate(String.valueOf(value), maxLength));
        } else {
            super.appendDetail(buffer, fieldName, value);
        }
    }
}
