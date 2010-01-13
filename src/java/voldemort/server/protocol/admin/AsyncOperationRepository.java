package voldemort.server.protocol.admin;

import org.apache.commons.collections.map.LRUMap;

/**
 * Extends LRUMap so that only <em>completed</em> operations may be removed from
 * the map LRUMap is in this case can only be constructed with a specified size
 * and will only be constructed with removeUntilScanable flag as true.
 *
 * TODO: replace {@link org.apache.commons.collections.map.LRUMap} with one based on {@link java.util.LinkedHashMap}.
 * TODO: rename this to something more sensible (AsyncOperationMap/Cache?)
 * 
 * @author afeinberg
 */
public class AsyncOperationRepository extends LRUMap {

    private static final long serialVersionUID = 1L;

    /**
     * Create a new repository for async operations.
     * 
     * @param maxSize Maximum size of repository
     */
    public AsyncOperationRepository(int maxSize) {
        super(maxSize, true);
    }

    @Override
    protected boolean removeLRU(LinkEntry entry) {
        AsyncOperation operation = (AsyncOperation) entry.getValue();
        return operation.getStatus().isComplete();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for(Object value: values()) {
            AsyncOperation as = (AsyncOperation) value;
            sb.append(as.getStatus().toString()).append(";");
        }

        return sb.toString();
    }
}
