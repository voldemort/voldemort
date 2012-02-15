package voldemort.store.stats;

import voldemort.annotations.concurrency.NotThreadsafe;

import java.util.Arrays;

/**
 * A class for computing percentiles based on a histogram
 */
@NotThreadsafe
public class Histogram {
    
    private final int nBuckets;
    private final int step;
    private final int[] buckets;
    private final int[] bounds;
    private int size;

    /**
     * 
     * @param nBuckets
     * @param step
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
     * 
     */
    public void reset() {
        Arrays.fill(buckets, 0);
        size = 0;
    }

    /**
     * 
     * @param data
     */
    public void insert(int data) {
        int index = findBucket(data);
        assert(index != -1);
        buckets[index]++;
        size++;
    }
    
    public int getQuantile(double quantile) {
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
    
    private int findBucket(int needle) {
        int max = step * nBuckets;
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
    
    private int compareToBucket(int bucket, int needle) {
        int low = bounds[bucket];
        int high = low + step;
        if (low <= needle && high > needle) {
            return 0;
        } else if(low > needle) {
            return 1;
        } else {
            return -1;
        }
    }
}
