package voldemort.store.stats;

import java.util.Arrays;

import org.apache.log4j.Logger;

import voldemort.annotations.concurrency.Threadsafe;

/**
 * A class for computing percentiles based on a histogram. Values are bucketed
 * by a configurable bound (e.g., 0-1, 1-2, 2-3). When a value is inserted,
 * perform a binary search to find the correct bucket.
 * 
 * 
 */
@Threadsafe
public class Histogram {

    private final int nBuckets;
    private final int step;
    private final int[] buckets;
    private final int[] bounds;
    private int size;
    private static final Logger logger = Logger.getLogger(Histogram.class);

    /**
     * Initialize an empty histogram
     * 
     * @param nBuckets The number of buckets to use
     * @param step The size of each bucket
     */
    public Histogram(int nBuckets, int step) {
        this.nBuckets = nBuckets;
        this.step = step;
        this.buckets = new int[nBuckets];
        this.bounds = new int[nBuckets];
        init();
    }

    protected void init() {
        int bound = 0;
        for(int i = 0; i < nBuckets; i++, bound += step) {
            bounds[i] = bound;
        }
        reset();
    }

    /**
     * Reset the histogram back to empty (set all values to 0)
     */
    public synchronized void reset() {
        Arrays.fill(buckets, 0);
        size = 0;
    }

    /**
     * Insert a value into the right bucket of the histogram. If the value is
     * larger than any bound, insert into the last bucket
     * 
     * @param data The value to insert into the histogram
     */
    public synchronized void insert(long data) {
        int index = findBucket(data);
        if(index == -1) {
            logger.error(data + " can't be bucketed, is invalid!");
        }
        buckets[index]++;
        size++;
    }

    /**
     * Find the a value <em>n</em> such that the percentile falls within [
     * <em>n</em>, <em>n + step</em>)
     * 
     * @param quantile The percentile to find
     * @return Lower bound associated with the percentile
     */
    public synchronized int getQuantile(double quantile) {
        int total = 0;
        for(int i = 0; i < nBuckets; i++) {
            total += buckets[i];
            double currQuantile = ((double) total) / ((double) size);
            if(currQuantile >= quantile) {
                return bounds[i];
            }
        }
        return 0;
    }

    private int findBucket(long needle) {
        long max = step * nBuckets;
        if(needle > max) {
            return nBuckets - 1;
        }
        int low = 0;
        int high = nBuckets - 1;
        while(low <= high) {
            int mid = (low + high) / 2;
            int cmp = compareToBucket(mid, needle);
            if(cmp == 0) {
                return mid;
            } else if(cmp > 0) {
                high = mid - 1;
            } else if(cmp < 0) {
                low = mid + 1;
            }
        }
        return -1;
    }

    private int compareToBucket(int bucket, long needle) {
        int low = bounds[bucket];
        int high = low + step;
        if(low <= needle && high > needle) {
            return 0;
        } else if(low > needle) {
            return 1;
        } else {
            return -1;
        }
    }
}
