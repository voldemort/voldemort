package voldemort.utils;

import java.io.Serializable;

/**
 * @param T the type of object that is being wrapped
 * @param K the type of value used to identify the object
 * 
 *        Immutable wrapper around an object that adds an id.
 */
public class Identified<T, K extends Serializable> implements Identifiable<K> {

    private final T _value;
    private final K _id;

    public Identified(T object, K id) {
        _value = object;
        _id = id;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.linkedin.common.entity.Identifiable#getId()
     */
    public K getId() {
        return _id;
    }

    public T getValue() {
        return _value;
    }
}
