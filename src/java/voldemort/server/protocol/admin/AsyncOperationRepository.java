package voldemort.server.protocol.admin;

import org.apache.commons.collections.map.LRUMap;

/**
 * Extends LRUMap so that only <em>completed</em> operations may be removed from the map
 * LRUMap is in this case can only be constructed with a specified size and will only be
 * constructed with removeUntilScanable flag as true.
 *
 * @author afeinberg
 */
public class AsyncOperationRepository extends LRUMap {
    /**
     * Create a new repository for async operations.
     * @param maxSize Maximum size of repository
     */
    public AsyncOperationRepository(int maxSize)  {
        super(maxSize, true);
    }

    @Override
    protected boolean removeLRU(LinkEntry entry) {
        AsyncOperation operation = (AsyncOperation) entry.getValue();
        return operation.getComplete();
    }
}
