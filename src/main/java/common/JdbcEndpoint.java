package common;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class JdbcEndpoint {

    private final String url;
    private final String user;
    private final String password;

    public JdbcEndpoint(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public final String getUrl() {
        return this.url;
    }

    public final String getUser() {
        return this.user;
    }

    public final String getPassword() {
        return this.password;
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString() {
        final ReflectionToStringBuilder reflectionToStringBuilder =
                new ReflectionToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);

        // explicitly exclude password, because we do not want to see password in logs
        reflectionToStringBuilder.setExcludeFieldNames(new String[] {"password"});

        return reflectionToStringBuilder.toString();
    }

}
