package voldemort.collections;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author jko
 * 
 *         Analogous to ListIterator, except for us with a map-backed list. The
 *         main difference is that these lists are not indexed by sequential
 *         number, but instead referred to by an id whose order has nothing to
 *         do with the position of the element in the list.
 * 
 * @param <K> the type of the key used by the underlying map
 * @param <E> the type of the elements being stored
 */
public interface MappedListIterator<K, E> extends Iterator<E> {

    /**
     * Inserts the specified element into the list (optional operation).
     */
    void add(E e);

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#hasNext()
     */
    boolean hasNext();

    /**
     * Returns true if this list iterator has more elements when traversing the
     * list in the reverse direction. (In other words, returns true if previous
     * would return an element rather than throwing an exception.)
     * 
     * @return true if the list iterator has more elements when traversing the
     *         list in the reverse direction
     */
    boolean hasPrevious();

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     */
    E next();

    /**
     * Returns the id associated with the next element in the list, or null if
     * the list iterator is at the end of the list.
     * 
     * @return the id associated with the next element in the list
     */
    K nextId();

    /**
     * Returns the previous element in the list. This method may be called
     * repeatedly to iterate through the list backwards, or intermixed with
     * calls to next to go back and forth. (Note that alternating calls to next
     * and previous will return the same element repeatedly.)
     * 
     * @return the previous element in the list
     * 
     * @throws NoSuchElementException - if the iteration has no previous
     *         element.
     */
    E previous();

    /**
     * Returns the id associated with the previous element in the list, or null
     * if the list iterator is at the beginning of the list.
     * 
     * @return the id associated with the previous element in the list
     */
    K previousId();

    /**
     * Returns the id for the last element that was returned by next or
     * previous.
     * 
     * @return the id for the last element that was returned by next or previous
     * @throws IllegalStateException if neither next nor previous has been
     *         called, or remove or add has been called after the last call to
     *         next or previous
     */
    K lastId();

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#remove()
     */
    void remove();

    /**
     * Replaces the last element returned by next or previous with the specified
     * element (optional operation). This call can be made only if neither
     * ListIterator.remove nor ListIterator.add have been called after the last
     * call to next or previous.
     * 
     * @param e the element with which to replace the last element returned by
     *        next or previous.
     * 
     * @throws UnsupportedOperationException if the set operation is not
     *         supported by this list iterator.
     * @throws ClassCastException if the class of the specified element prevents
     *         it from being added to this list.
     * @throws IllegalArgumentException if some aspect of the specified element
     *         prevents it from being added to this list.
     * @throws IllegalStateException if neither next nor previous have been
     *         called, or remove or add have been called after the last call to
     *         next or previous.
     */
    void set(E e);
}
