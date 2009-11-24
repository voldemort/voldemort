package voldemort.store.views;

/**
 * A view that fails all operations. Override one of the methods to add either
 * reads or writes.
 * 
 * @author jay
 * 
 * @param <V> The type of objects in the view
 * @param <S> The type of objects in the store
 */
public class AbstractViewTransformation<V, S> implements ViewTransformation<V, S> {

    public V fromStore(S s) {
        throw new UnsupportedViewOperation("Read attempt on write-only view!");
    }

    public S fromView(V v) {
        throw new UnsupportedViewOperation("Write attempt on read-only view!");
    }

}
