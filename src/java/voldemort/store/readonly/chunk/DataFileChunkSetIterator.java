/*
 * Copyright 2011 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.readonly.chunk;

import java.io.IOException;
import java.nio.ByteBuffer;
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
    protected boolean coalesceCollided;

    // Number of k-v pairs which have collided and are in one bucket
    private short tupleCount;

    private ReadWriteLock fileModificationLock;

    public DataFileChunkSetIterator(DataFileChunkSet dataFileChunkSet, boolean coalesceCollided) {
        this(dataFileChunkSet, coalesceCollided, null);
    }

    /**
     * Iterator over the data file chunks
     * 
     * @param dataFileChunkSet Set of data chunk files
     * @param coalesceCollided Whether we want to consider collided entries as
     *        one?
     * @param fileModificationLock RW lock to lock before starting the iterator
     */
    public DataFileChunkSetIterator(DataFileChunkSet dataFileChunkSet,
                                    boolean coalesceCollided,
                                    ReadWriteLock fileModificationLock) {
        this.dataFileChunkSet = dataFileChunkSet;
        this.currentChunk = 0;
        this.currentOffsetInChunk = 0;
        this.tupleCount = 0;
        this.chunksFinished = false;
        this.fileModificationLock = fileModificationLock;
        this.coalesceCollided = coalesceCollided;
        if(this.fileModificationLock != null)
            this.fileModificationLock.readLock().lock();
    }

    public void close() {
        if(this.fileModificationLock != null)
            this.fileModificationLock.readLock().unlock();
    }

    public abstract T next();

    protected DataFileChunk getCurrentChunk() {
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

        if(!coalesceCollided && tupleCount == 0) {
            // Update the tuple count
            ByteBuffer numKeyValsBuffer = ByteBuffer.allocate(ByteUtils.SIZE_OF_SHORT);
            try {
                getCurrentChunk().read(numKeyValsBuffer, currentOffsetInChunk);
            } catch(IOException e) {
                logger.error("Error while reading tuple count in iterator", e);
                return false;
            }
            tupleCount = numKeyValsBuffer.getShort(0);
            currentOffsetInChunk += ByteUtils.SIZE_OF_SHORT;
        }
        return true;
    }

    public void updateOffset(long updatedOffset) {
        if(!coalesceCollided)
            tupleCount--;
        currentOffsetInChunk = updatedOffset;
        hasNext();
    }

    public void remove() {
        throw new UnsupportedOperationException("Cannot remove from these read-only data chunks");
    }
}
