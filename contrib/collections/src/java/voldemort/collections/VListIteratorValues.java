package voldemort.collections;


/**
 * @author jko
 * 
 *         Wrapper around VListIterator that strips out the version information.
 *         Only useful when no further storage operations will be done on these
 *         values.
 */
public class VListIteratorValues<E> implements MappedListIterator<Integer, E> {

    private final VListIterator<E> _listIterator;

    public VListIteratorValues(VListIterator<E> vListIterator) {
        _listIterator = vListIterator;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#add(java.lang.Object)
     */
    public void add(E arg0) {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#hasNext()
     */
    public boolean hasNext() {
        return _listIterator.hasNext();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#hasPrevious()
     */
    public boolean hasPrevious() {
        return _listIterator.hasPrevious();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#next()
     */
    public E next() {
        return _listIterator.next().getValue();
    }

    public Integer nextId() {
        return _listIterator.nextId();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#previous()
     */
    public E previous() {
        return _listIterator.previous().getValue();
    }

    public Integer previousId() {
        return _listIterator.previousId();
    }

    public Integer lastId() {
        return _listIterator.lastId();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#remove()
     */
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#set(java.lang.Object)
     */
    public void set(E arg0) {
        throw new UnsupportedOperationException();
    }

}
