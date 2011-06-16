package voldemort.store.stats;

import java.text.NumberFormat;

/**
 * Statistics about the keys and values in a store
 * 
 * 
 */
public class DataSetStats {

    private long numEntries = 0;
    private long keyBytes = 0;
    private long valueBytes = 0;
    private int minKeySize = Integer.MAX_VALUE;
    private int minValueSize = Integer.MAX_VALUE;
    private int maxKeySize = Integer.MIN_VALUE;
    private int maxValueSize = Integer.MIN_VALUE;

    public DataSetStats() {}

    public void countEntry(int keySize, int valueSize) {
        this.numEntries++;
        this.keyBytes += keySize;
        this.valueBytes += valueSize;
        if(keySize > this.maxKeySize)
            this.maxKeySize = keySize;
        if(keySize < this.minKeySize)
            this.minKeySize = keySize;
        if(valueSize > this.maxValueSize)
            this.maxValueSize = valueSize;
        if(valueSize < this.minValueSize)
            this.minValueSize = valueSize;
    }

    public long getTotalEntries() {
        return numEntries;
    }

    public long getTotalKeyBytes() {
        return keyBytes;
    }

    public long getTotalValueBytes() {
        return valueBytes;
    }

    public int getMaxKeySize() {
        return maxKeySize;
    }

    public int getMaxValueSize() {
        return maxValueSize;
    }

    public int getMinKeySize() {
        return minKeySize;
    }

    public int getMinValueSize() {
        return minValueSize;
    }

    public long getTotalBytes() {
        return getTotalKeyBytes() + getTotalValueBytes();
    }

    public double getAvgKeySize() {
        return getTotalKeyBytes() / (double) getTotalEntries();
    }

    public double getAvgValueSize() {
        return getTotalValueBytes() / (double) getTotalEntries();
    }

    public double getAvgEntrySize() {
        return getTotalBytes() / (double) getTotalEntries();
    }

    public void add(DataSetStats stats) {
        this.numEntries += stats.getTotalEntries();
        this.keyBytes += stats.getTotalKeyBytes();
        this.valueBytes = stats.getTotalValueBytes();
        this.minKeySize = Math.min(this.minKeySize, stats.getMinKeySize());
        this.minValueSize = Math.min(this.minValueSize, stats.getMinValueSize());
        this.maxKeySize = Math.max(this.maxKeySize, stats.getMaxKeySize());
        this.maxValueSize = Math.max(this.maxValueSize, stats.getMaxValueSize());
    }

    @Override
    public String toString() {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMaximumFractionDigits(1);
        return "Total entries: " + getTotalEntries() + "\n" + "Total bytes: " + getTotalBytes()
               + "\n" + "Avg. key size: " + nf.format(getAvgKeySize()) + "\n" + "Avg. value size: "
               + nf.format(getAvgValueSize()) + "\n" + "Smallest key size: " + getMinValueSize()
               + "\n" + "Largest key size: " + getMinValueSize() + "\n" + "Smallest value size: "
               + getMinValueSize() + "\n" + "Largest value size: " + getMaxValueSize();
    }

}
