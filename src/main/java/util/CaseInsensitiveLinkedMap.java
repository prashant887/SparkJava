package util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.collections4.map.AbstractLinkedMap;
import org.apache.commons.collections4.map.CaseInsensitiveMap;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class CaseInsensitiveLinkedMap<K extends String, V> extends AbstractLinkedMap<K, V> implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    private static final Pattern NON_ALNUM = Pattern.compile("[^A-Za-z0-9]");

    private static final LoadingCache<String, String> cache = CacheBuilder.newBuilder()
            .maximumSize(10000)
            .build(new CacheLoader<String, String>() {
                @Override
                public String load(String key) {
                    // using english locale because of "Turkish locale bug"
                    return NON_ALNUM.matcher(key.toLowerCase(Locale.ENGLISH)).replaceAll("_");
                }
            });

    /**
     * Constructs a new empty map with default size and load factor.
     */
    public CaseInsensitiveLinkedMap() {
        super(DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_THRESHOLD);
    }

    public CaseInsensitiveLinkedMap(final Map<? extends K, ? extends V> map) {
        super(map);
    }

    /**
     * Overrides convertKey() from {@link } to convert keys to
     * lower case and non-alphanumeric characters to underscores
     * <p>
     * Returns {@link } if key is null or fails to convert.
     *
     * @param key  the key convert
     * @return the converted key
     */
    @Override
    protected Object convertKey(final Object key) {
        try {
            if (key != null) {
                return cache.get(key.toString());
            }
        } catch (ExecutionException e) {
            System.out.println("Could not convert key :"+ e.getMessage());
        }
        return AbstractLinkedMap.NULL;
    }

    /**
     * Clones the map without cloning the keys or values.
     *
     * @return a shallow clone
     */
    @Override // required by Cloneable
    public CaseInsensitiveMap<K, V> clone() {
        return (CaseInsensitiveMap<K, V>) super.clone();
    }

    // required by Serializable
    /**
     * Write the map out using a custom routine.
     */
    private void writeObject(final ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        doWriteObject(out);
    }

    // required by Serializable
    /**
     * Read the map in using a custom routine.
     */
    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        doReadObject(in);
    }

}
