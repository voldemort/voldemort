package voldemort.store.views;

import voldemort.store.Store;

/**
 * A view that fails all operations. Override one of the methods to add either
 * reads or writes.
 * 
 * 
 * @param <K> The type of the key
 * @param <V> The type of objects in the view
 * @param <S> The type of objects in the store
 * @param <T> The type of transforms
 */
public class AbstractViewTransformation<K, V, S, T> implements View<K, V, S, T> {

    public V storeToView(Store<K, S, T> store, K k, S s, T t) {
        throw new UnsupportedViewOperationException("Read attempt on write-only view!");
    }

    public S viewToStore(Store<K, S, T> store, K k, V v, T t) {
        throw new UnsupportedViewOperationException("Write attempt on read-only view!");
    }

}
