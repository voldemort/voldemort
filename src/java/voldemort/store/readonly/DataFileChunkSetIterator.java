package voldemort.store.readonly;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.log4j.Logger;

import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;

abstract class DataFileChunkSetIterator<T> implements ClosableIterator<T> {

    private static Logger logger = Logger.getLogger(DataFileChunkSetIterator.class);

    private DataFileChunkSet dataFileChunkSet;
    private int currentChunk;
    private long currentOffsetInChunk;
    private boolean chunksFinished;
    protected boolean ignoreCollisions;

    // Number of k-v pairs which have collided and are in one bucket
    private int tupleCount;

    private ReadWriteLock fileModificationLock;

    public DataFileChunkSetIterator(DataFileChunkSet dataFileChunkSet, boolean ignoreCollisions) {
        this(dataFileChunkSet, ignoreCollisions, null);
    }

    /**
     * Iterator over the data file chunks
     * 
     * @param dataFileChunkSet Set of data chunk files
     * @param ignoreCollisions Whether we want to consider collided entries as
     *        one?
     * @param fileModificationLock RW lock to lock before starting the iterator
     */
    public DataFileChunkSetIterator(DataFileChunkSet dataFileChunkSet,
                                    boolean ignoreCollisions,
                                    ReadWriteLock fileModificationLock) {
        this.dataFileChunkSet = dataFileChunkSet;
        this.currentChunk = 0;
        this.currentOffsetInChunk = 0;
        this.tupleCount = 0;
        this.chunksFinished = false;
        this.fileModificationLock = fileModificationLock;
        this.ignoreCollisions = ignoreCollisions;
        if(this.fileModificationLock != null)
            this.fileModificationLock.readLock().lock();
    }

    public void close() {
        if(this.fileModificationLock != null)
            this.fileModificationLock.readLock().unlock();
    }

    public abstract T next();

    protected FileChannel getCurrentChunk() {
        return this.dataFileChunkSet.dataFileFor(currentChunk);
    }

    protected long getCurrentOffsetInChunk() {
        return this.currentOffsetInChunk;
    }

    public boolean hasNext() {
        if(chunksFinished)
            return false;

        // Jump till you reach the first non-zero data size file or end
        if(currentOffsetInChunk >= this.dataFileChunkSet.getDataFileSize(currentChunk)) {
            // Time to jump to another chunk with a non-zero size data file
            currentChunk++;
            currentOffsetInChunk = 0;

            // Jump over all zero size data files
            while(currentChunk < this.dataFileChunkSet.getNumChunks()
                  && this.dataFileChunkSet.getDataFileSize(currentChunk) == 0) {
                currentChunk++;
            }

            if(currentChunk == this.dataFileChunkSet.getNumChunks()) {
                chunksFinished = true;
                return false;
            }
        }

        if(!ignoreCollisions && tupleCount == 0) {
            // Update the tuple count
            ByteBuffer numKeyValsBuffer = ByteBuffer.allocate(ByteUtils.SIZE_OF_BYTE);
            try {
                getCurrentChunk().read(numKeyValsBuffer, currentOffsetInChunk);
            } catch(IOException e) {
                logger.error("Error while reading tuple count in iterator", e);
                return false;
            }
            tupleCount = numKeyValsBuffer.get(0) & ByteUtils.MASK_11111111;
            currentOffsetInChunk += ByteUtils.SIZE_OF_BYTE;
        }
        return true;
    }

    public void updateOffset(long updatedOffset) {
        if(!ignoreCollisions)
            tupleCount--;
        currentOffsetInChunk = updatedOffset;
        hasNext();
    }

    public void remove() {
        throw new UnsupportedOperationException("Cannot remove from these read-only data chunks");
    }
}
