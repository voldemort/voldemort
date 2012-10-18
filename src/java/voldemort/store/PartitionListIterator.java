/*
 * Copyright 2008-2012 LinkedIn, Inc
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
package voldemort.store;

import java.util.List;
import java.util.NoSuchElementException;

import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
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
        Utils.notNull(partitionsToFetch);
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
            this.partitionIterator = storageEngine.entries(this.partitionsToFetch.get(currentIndex));
            currentIndex++;
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
