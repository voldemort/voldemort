package voldemort.store.bdb;

import voldemort.utils.ClosableIterator;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseException;

abstract class BdbIterator<T> implements ClosableIterator<T> {

    private volatile boolean isOpen;
    final Cursor cursor;
    final BdbStorageEngine bdbEngine;

    BdbIterator(Cursor cursor, BdbStorageEngine bdbEngine) {
        this.cursor = cursor;
        this.bdbEngine = bdbEngine;
        isOpen = true;

    }

    public final void close() {
        try {
            if(isOpen) {
                cursor.close();
                isOpen = false;
            }
        } catch(DatabaseException e) {
            bdbEngine.getLogger().error(e);
        }
    }

    public final void remove() {
        throw new UnsupportedOperationException("No removal");
    }

    @Override
    protected final void finalize() {
        if(isOpen) {
            bdbEngine.getLogger().error("Failure to close cursor, will be forcibly closed.");
            close();
        }
    }
}