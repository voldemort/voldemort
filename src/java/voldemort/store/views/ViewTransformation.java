package voldemort.store.views;

/**
 * The interface for defining a view. A separate transformation may be needed
 * for the keys and the values, and one can do just one or the other if needed.
 * 
 * This interface provides a translation from the view's type to the target
 * store's type and vice versa. If one direction is not supported, the
 * unimplemented method should throw an
 * {@link voldemort.store.views.UnsupportedViewOperation}.
 * 
 * @author jay
 * 
 * @param <V> The type of things in the view
 * @param <S> The type of things in the store
 */
public interface ViewTransformation<V, S> {

    /**
     * Translate from the store type to the view type
     * 
     * @param s The value for the store
     * @return The value for the view
     * @throws UnsupportedViewOperation If this direction of translation is not
     *         allowed
     */
    public V fromStore(S s) throws UnsupportedViewOperation;

    /**
     * Translate from the view type to the store type
     * 
     * @param v The view type
     * @return The store type
     * @throws UnsupportedViewOperation If this direction of translation is not
     *         allowed
     */
    public S fromView(V v) throws UnsupportedViewOperation;

}
