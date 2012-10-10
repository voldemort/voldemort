package voldemort.store;

import java.util.List;
import java.util.NoSuchElementException;

import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

/**
 * Iterator that uses efficient partition scan to iterate across a list of
 * supplied partitions
 * 
 */
public class PartitionListIterator implements ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

    StorageEngine<ByteArray, byte[], byte[]> storageEngine;
    List<Integer> partitionsToFetch;
    ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> partitionIterator;
    int currentIndex;

    public PartitionListIterator(StorageEngine<ByteArray, byte[], byte[]> storageEngine,
                                 List<Integer> partitionsToFetch) {
        this.storageEngine = storageEngine;
        this.partitionsToFetch = partitionsToFetch;
        this.currentIndex = 0;
    }

    public boolean hasNext() {
        // do we have more elements in the current partition we are serving?
        if(this.partitionIterator != null && this.partitionIterator.hasNext())
            return true;
        // if not, find the next non empty partition
        while((currentIndex < partitionsToFetch.size())) {
            // close the previous iterator
            if(this.partitionIterator != null)
                this.partitionIterator.close();
            // advance to the next partition
            this.partitionIterator = storageEngine.entries(this.partitionsToFetch.get(currentIndex++));
            if(this.partitionIterator.hasNext())
                return true;
        }
        return false;
    }

    public Pair<ByteArray, Versioned<byte[]>> next() {
        if(!hasNext())
            throw new NoSuchElementException("End of partition entries stream");
        return this.partitionIterator.next();
    }

    public void remove() {
        throw new UnsupportedOperationException("Removal not supported");
    }

    @Override
    protected final void finalize() {
        close();
    }

    public void close() {
        if(partitionIterator != null) {
            partitionIterator.close();
        }
    }
}
