package voldemort.collections;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;

import voldemort.client.StoreClient;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

/**
 * Linked list stack implementation on top of voldemort. Nodes within the stack
 * are identified by their id. Only adding and setting nodes are allowed.
 * Removing is unsupported due to possible stack corruption.
 * 
 * The voldemort store configuration must use JSON serialization. The key must
 * be defined as a JSON object as the following: {"value" : <JSON serialization
 * format of K>, "id" : "int32"} The value must be defined as a JSON object as
 * the following: {"id" : "int32", "nextId" : "int32", "previousId" : "int32",
 * "stable" : "boolean", "value" : <JSON serialization format of E>}
 * 
 * @param <K> the type of key used to identify this stack. Must conform to valid
 *        voldemort JSON formats: http://project-voldemort.com/design.php
 * @param <E> the type of element being stored
 */
public class VStack<K, E> implements Queue<E> {

    public static final Integer NULL_ID = Integer.MIN_VALUE + 1;

    // could be used to store the size of the list
    public static final Integer SIZE_ID = Integer.MIN_VALUE + 2;

    private final K _key;
    private final StoreClient<Map<String, Object>, Map<String, Object>> _storeClient;

    /**
     * 
     * @param key
     * @param storeClient
     */
    public VStack(K key, StoreClient<Map<String, Object>, Map<String, Object>> storeClient) {
        if(key == null)
            throw new NullPointerException("key must not be null");

        _key = key;
        _storeClient = storeClient;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#clear()
     */
    public void clear() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#contains(java.lang.Object)
     */
    public boolean contains(Object arg0) {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#containsAll(java.util.Collection)
     */
    public boolean containsAll(Collection<?> arg0) {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#isEmpty()
     * 
     * Runtime: 1 voldemort get operation
     */
    public boolean isEmpty() {
        VListKey<K> newKey = new VListKey<K>(_key, 0);
        Versioned<Map<String, Object>> firstNode = _storeClient.get(newKey.mapValue());
        return firstNode == null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#iterator()
     */
    public Iterator<E> iterator() {
        return new VListIteratorValues<E>(listIterator(0));
    }

    /*
     * Returns a versioned iterator that starts at a specified id.
     */
    public Iterator<Versioned<E>> iterator(int id) {
        return listIterator(id);
    }

    public MappedListIterator<Integer, Versioned<E>> listIterator() {
        return listIterator(0);
    }

    /*
     * Returns a VListIterator<E> that starts at a specified id.
     * 
     * @throws IndexOutOfBoundsException if the id does not exist in the list
     */
    public VListIterator<E> listIterator(int id) {
        return new VListIterator<E>(this, id);
    }

    /**
     * Returns a ListIterator that ends at a specified id. The cursor position
     * is just after the id.
     * 
     * @throws IndexOutOfBoundsException if the id does not exist
     */
    public VListIterator<E> previousListIterator(int id) {
        return new VListIterator<E>(this, id, false);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#remove(java.lang.Object)
     */
    public boolean remove(Object arg0) {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#removeAll(java.util.Collection)
     */
    public boolean removeAll(Collection<?> arg0) {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#retainAll(java.util.Collection)
     */
    public boolean retainAll(Collection<?> arg0) {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#size()
     * 
     * Runtime: O(n) gets (WARNING -- very very slow. Only use for debugging
     * purposes)
     */
    public int size() {
        int result = 0;
        for(@SuppressWarnings("unused")
        E element: this) {
            result++;
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#toArray()
     */
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#toArray(T[])
     */
    public <T> T[] toArray(T[] arg0) {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc) Adds a new element into the VStack with a new version.
     */
    public boolean add(E e) {
        if(!offer(e))
            throw new IllegalStateException();
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#addAll(java.util.Collection)
     */
    public boolean addAll(Collection<? extends E> elements) {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Queue#element()
     */
    public E element() {
        E result = peek();
        if(result == null)
            throw new NoSuchElementException();

        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Queue#offer(java.lang.Object)
     * 
     * Runtime: worst case: 2 gets, 5 puts
     */
    public boolean offer(E e) {
        if(e == null)
            throw new NullPointerException("null objects are not allowed");
        AddNodeAction<K, E> addAction = new AddNodeAction<K, E>(_key, e);
        try {
            return _storeClient.applyUpdate(addAction);
        } catch(IndexOutOfBoundsException ex) {
            // over-capacity
            return false;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Queue#peek()
     */
    public E peek() {
        Versioned<E> versioned = peekVersioned();
        if(versioned == null) {
            return null;
        } else {
            return versioned.getValue();
        }
    }

    public Versioned<E> peekVersioned() {
        Versioned<VListNode<E>> node = getListNode(0);
        if(node == null) {
            return null;
        } else {
            return new Versioned<E>(node.getValue().getValue(), node.getVersion());
        }
    }

    /**
     * Get the ver
     * 
     * @param id
     * @return
     */
    public Versioned<E> getVersionedById(int id) {
        Versioned<VListNode<E>> listNode = getListNode(id);
        if(listNode == null)
            throw new IndexOutOfBoundsException();
        return new Versioned<E>(listNode.getValue().getValue(), listNode.getVersion());
    }

    /**
     * Get the value associated with the id. This method strips off all version
     * information and is only useful when no further storage operations will be
     * done on this value.
     * 
     * @param id
     * @return
     */
    public E getById(int id) {
        return getVersionedById(id).getValue();
    }

    /**
     * Put the given value to the appropriate id in the stack, using the version
     * of the current list node identified by that id.
     * 
     * @param id
     * @param element element to set
     * @return element that was replaced by the new element
     * @throws ObsoleteVersionException when an update fails
     */
    public E setById(final int id, final E element) {
        VListKey<K> key = new VListKey<K>(_key, id);
        UpdateElementById<K, E> updateElementAction = new UpdateElementById<K, E>(key, element);

        if(!_storeClient.applyUpdate(updateElementAction))
            throw new ObsoleteVersionException("update failed");

        return updateElementAction.getResult();
    }

    public E setById(int id, Versioned<E> element) {
        VListKey<K> key = new VListKey<K>(_key, id);
        UpdateElementById<K, E> updateElementAction = new UpdateElementById<K, E>(key, element);

        if(!_storeClient.applyUpdate(updateElementAction))
            throw new ObsoleteVersionException("update failed");

        return updateElementAction.getResult();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Queue#poll()
     */
    public E poll() {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Queue#remove()
     */
    public E remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * 
     * @param id
     * @return the VListNode identified by id. If it doesn't exist, return null
     */
    Versioned<VListNode<E>> getListNode(int id) {
        VListKey<K> key = new VListKey<K>(_key, id);
        Versioned<Map<String, Object>> resultMap = _storeClient.get(key.mapValue());
        if(resultMap == null)
            return null;

        VListNode<E> listNode = VListNode.<E> valueOf(resultMap.getValue());
        Versioned<VListNode<E>> versionedNode = new Versioned<VListNode<E>>(listNode,
                                                                            resultMap.getVersion());
        return versionedNode;
    }
}
