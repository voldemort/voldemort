package voldemort.store.db4o;

import java.util.Iterator;

import voldemort.store.PersistenceFailureException;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;

public class Db4oEntriesIterator<Key, Value> implements ClosableIterator<Pair<Key, Value>> {

    private Iterator<Db4oKeyValuePair<Key, Value>> iterator;
    private volatile boolean isOpen;

    public Db4oEntriesIterator(Db4oKeyValueProvider<Key, Value> provider) {
        this.iterator = provider.getAll().iterator();
        isOpen = this.iterator != null;
    }

    /*
     * protected abstract T get(DatabaseEntry key, DatabaseEntry value);
     * 
     * protected abstract void moveCursor(DatabaseEntry key, DatabaseEntry
     * value) throws DatabaseException;
     */

    public final boolean hasNext() {
        return iterator.hasNext();
    }

    public final Pair<Key, Value> next() {
        if(!isOpen)
            throw new PersistenceFailureException("Call to next() on a closed iterator.");
        return iterator.next().toVoldemortPair();
    }

    public final void remove() {
        throw new UnsupportedOperationException("No removal y'all.");
    }

    public final void close() {
        isOpen = false;
    }

    @Override
    protected final void finalize() {
        if(isOpen) {
            // logger.error("Failure to close cursor, will be forcibly closed.");
            close();
        }

    }

}
