package voldemort.server.scheduler;

/**
 * 
 * Interface to be implemented by all jobs that perform scans over the database,
 * acquiring scan permits
 * 
 */
public class ScanProgress {

    private volatile long totalItemsScanned;
    private long lastReturnedCount;

    public ScanProgress() {
        totalItemsScanned = 0;
        lastReturnedCount = 0;
    }

    /**
     * Return the number of entries scanned since last call
     * 
     * @return
     */
    public long getNumEntriesScanned() {
        long itemsScanned = totalItemsScanned - lastReturnedCount;
        lastReturnedCount = totalItemsScanned;
        return itemsScanned;
    }

    /**
     * Updates the items scanned. volatile makes sure the changes hit memory
     */
    public void itemScanned() {
        totalItemsScanned++;
    }

    /**
     * Returns the total number of entries scanned
     * 
     * @return
     */
    public long getTotalItemsScanned() {
        return totalItemsScanned;
    }
}
