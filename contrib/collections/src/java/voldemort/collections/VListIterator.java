package voldemort.collections;

import java.util.NoSuchElementException;

import voldemort.versioning.Versioned;

/**
 * @author jko
 * 
 */
public class VListIterator<E> implements MappedListIterator<Integer, Versioned<E>> {

    private final VStack<?, E> _stack;

    private Versioned<VListNode<E>> _nextNode = null;
    private boolean _isNextSet = false;

    private Versioned<VListNode<E>> _previousNode = null;
    private boolean _isPreviousSet = false;

    // last item returned by next or previous
    private int _lastId = VStack.NULL_ID;
    private LastCall _lastCall = null;

    /**
     * package private -- we don't want outsiders constructing this
     * 
     * Creates a ListIterator with a cursor position specified by id and next.
     * 
     * @param id - the id of the node to position the cursor
     * @param next - true if the cursor is to be placed directly before the
     *        node, false if the cursor is to be placed directly after the node
     */
    VListIterator(VStack<?, E> stack, int id, boolean next) {
        _stack = stack;

        if(next) {
            _nextNode = stack.getListNode(id);
            if(_nextNode == null) {
                if(id == 0)
                    _isPreviousSet = true;
                else
                    throw new IndexOutOfBoundsException();
            }
            _isNextSet = true;
        } else {
            _previousNode = stack.getListNode(id);
            if(_previousNode == null) {
                if(id == 0)
                    _isNextSet = true;
                else
                    throw new IndexOutOfBoundsException();
            }
            _isPreviousSet = true;
        }
    }

    VListIterator(VStack<?, E> stack, int id) {
        this(stack, id, true);
    }

    private void setPreviousNode() {
        if(!_isPreviousSet) {
            if(!_isNextSet)
                throw new IllegalStateException("either next or previous must be set at any given time");
            Integer previousId = _nextNode.getValue().getPreviousId();
            if(previousId != VStack.NULL_ID) {
                _previousNode = _stack.getListNode(previousId);
            } else {
                _previousNode = null;
            }

            _isPreviousSet = true;
        }
    }

    private void setNextNode() {
        if(!_isNextSet) {
            if(!_isPreviousSet)
                throw new IllegalStateException("either next or previous must be set at any given time");

            Integer nextId = _previousNode.getValue().getNextId();
            if(nextId != VStack.NULL_ID) {
                _nextNode = _stack.getListNode(nextId);
            } else {
                _nextNode = null;
            }

            _isNextSet = true;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#add(java.lang.Object)
     */
    public void add(Versioned<E> arg0) {
        _lastId = VStack.NULL_ID;
        _lastCall = LastCall.ADD;
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#hasNext()
     */
    public boolean hasNext() {
        setNextNode();
        return _nextNode != null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#hasPrevious()
     */
    public boolean hasPrevious() {
        setPreviousNode();
        return _previousNode != null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#next()
     */
    public Versioned<E> next() {
        if(!hasNext())
            throw new NoSuchElementException();

        Versioned<VListNode<E>> tmpNode = _nextNode;
        VListNode<E> tmpNodeValue = _nextNode.getValue();
        _lastId = tmpNodeValue.getId();
        _lastCall = LastCall.NEXT;

        if(tmpNodeValue.getNextId() != VStack.NULL_ID) {
            _nextNode = _stack.getListNode(tmpNodeValue.getNextId());
        } else {
            _nextNode = null;
        }

        _previousNode = tmpNode;
        _isPreviousSet = true;

        return new Versioned<E>(tmpNodeValue.getValue(), tmpNode.getVersion());
    }

    /**
     * Returns the id of the element that would be returned by a subsequent call
     * to next. Returns null if the list iterator is at the end of the list.
     */
    public Integer nextId() {
        if(hasNext())
            return _nextNode.getValue().getId();
        else
            return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#previous()
     */
    public Versioned<E> previous() {
        if(hasPrevious()) {
            E resultValue = _previousNode.getValue().getValue();
            _lastId = _previousNode.getValue().getId();
            _lastCall = LastCall.PREVIOUS;
            _nextNode = _previousNode;
            _isNextSet = true;
            _isPreviousSet = false;
            return new Versioned<E>(resultValue, _previousNode.getVersion());
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Returns the id of the element that would be returned by a subsequent call
     * to next. Returns null if the list iterator is at the end of the list.
     */
    public Integer previousId() {
        if(hasPrevious()) {
            return _previousNode.getValue().getId();
        } else {
            return null;
        }
    }

    public Integer lastId() {
        if(_lastCall != LastCall.NEXT && _lastCall != LastCall.PREVIOUS)
            throw new IllegalStateException("neither next() nor previous() has been called");
        return _lastId;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#remove()
     */
    public void remove() {
        _lastId = VStack.NULL_ID;
        _lastCall = LastCall.REMOVE;
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#set(java.lang.Object)
     * 
     * Replaces the last element returned by next or previous with the specified
     * element (optional operation). This call can be made only if neither
     * ListIterator.remove nor ListIterator.add have been called after the last
     * call to next or previous.
     */
    public void set(Versioned<E> element) {
        if(element == null)
            throw new NullPointerException("cannot set a null element");
        if(_lastCall != LastCall.NEXT && _lastCall != LastCall.PREVIOUS)
            throw new IllegalStateException("neither next() nor previous() has been called");

        _stack.setById(_lastId, element);

        afterSet(element.getValue());
    }

    /*
     * Set the value of the last element returned by next or previous,
     * discarding any version information
     */
    public void setValue(E element) {
        if(element == null)
            throw new NullPointerException("cannot set a null element");
        if(_lastId == VStack.NULL_ID)
            throw new IllegalStateException("neither next() nor previous() has been called");

        _stack.setById(_lastId, element);

        afterSet(element);
    }

    // helper function to be called after set() and setValue()
    private void afterSet(E element) {
        switch(_lastCall) {
            case NEXT:
                _previousNode = _stack.getListNode(_lastId);
                break;
            case PREVIOUS:
                _nextNode = _stack.getListNode(_lastId);
                break;
            default:
                throw new IllegalStateException();
        }
    }

    private enum LastCall {
        NEXT,
        PREVIOUS,
        ADD,
        REMOVE
    }
}
