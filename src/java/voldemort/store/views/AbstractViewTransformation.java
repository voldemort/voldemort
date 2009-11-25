package voldemort.store.views;

/**
 * A view that fails all operations. Override one of the methods to add either
 * reads or writes.
 * 
 * @author jay
 * 
 * @param <K> The type of the key
 * @param <V> The type of objects in the view
 * @param <S> The type of objects in the store
 */
public class AbstractViewTransformation<K, V, S> implements ViewTransformation<K, V, S> {

    public V fromStoreToView(K k, S s) {
        throw new UnsupportedViewOperationException("Read attempt on write-only view!");
    }

    public S fromViewToStore(K k, V v) {
        throw new UnsupportedViewOperationException("Write attempt on read-only view!");
    }

}
