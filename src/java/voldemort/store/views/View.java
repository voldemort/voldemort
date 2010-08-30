package voldemort.store.views;

import voldemort.annotations.Experimental;
import voldemort.store.Store;

/**
 * The interface for defining a view.
 * 
 * This interface provides a translation from the view's type to the target
 * store's type and vice versa. If one direction is not supported, the
 * unimplemented method should throw an
 * {@link voldemort.store.views.UnsupportedViewOperationException}.
 * 
 * 
 * @param <K> The type of the key
 * @param <V> The type of things in the view
 * @param <S> The type of things in the store
 * @param <T> The type of transform
 */
@Experimental
public interface View<K, V, S, T> {

    /**
     * Translate from the store type to the view type
     * 
     * @param targetStore The store behind the view
     * @param k The key
     * @param s The value for the store
     * @param t the transforms to be applied on s
     * @return The value for the view
     * @throws UnsupportedViewOperationException If this direction of
     *         translation is not allowed
     */
    public V storeToView(Store<K, S, T> targetStore, K k, S s, T t)
            throws UnsupportedViewOperationException;

    /**
     * Translate from the view type to the store type
     * 
     * @param targetStore The store behind the view
     * @param k The key
     * @param v The value
     * @param t the transforms to be applied on v
     * @return The store type
     * @throws UnsupportedViewOperationException If this direction of
     *         translation is not allowed
     */
    public S viewToStore(Store<K, S, T> targetStore, K k, V v, T t)
            throws UnsupportedViewOperationException;

}
