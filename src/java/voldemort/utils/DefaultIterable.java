package voldemort.utils;

import java.util.Iterator;

import com.google.common.collect.AbstractIterable;

/**
 * An iterable that always produces the given iterator
 * 
 * @author jay
 * 
 */
public class DefaultIterable<V> extends AbstractIterable<V> {

    private final Iterator<V> iterator;

    public DefaultIterable(Iterator<V> iterator) {
        this.iterator = iterator;
    }

    public Iterator<V> iterator() {
        return this.iterator;
    }

}
