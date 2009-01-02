package voldemort.store;

import com.google.common.base.Objects;

/**
 * An Entry in a Store representing a key and its value.
 * 
 * @author jay
 * 
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public class Entry<K, V> {

    private final K key;
    private final V value;

    public Entry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof Entry))
            return false;
        Entry<?, ?> e = (Entry<?, ?>) obj;
        return Objects.equal(key, e.getKey()) && Objects.equal(value, e.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key, value);
    }

    @Override
    public String toString() {
        return "Entry(key=" + key + ", value=" + value + ")";
    }

}
