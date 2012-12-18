package voldemort.server.niosocket;

import org.apache.commons.lang.mutable.MutableInt;

import voldemort.common.nio.CommBufferSizeStats;
import voldemort.common.nio.SelectorManager;
import voldemort.store.stats.Histogram;

/**
 * Encapsulates all the statistics about various metrics in the NIO Network
 * layer
 * 
 */
public class NioSelectorManagerStats {

    private static long SELECTOR_STATS_RESET_INTERVAL = 60000;

    private MutableInt numActiveConnections;

    private Histogram selectTimeMsHistogram;

    private Histogram selectCountHistogram;

    private Histogram processingTimeMsHistogram;

    private CommBufferSizeStats serverCommBufferStats;

    public NioSelectorManagerStats() {
        this.numActiveConnections = new MutableInt(0);
        this.serverCommBufferStats = new CommBufferSizeStats();

        // Theoretically, the delay can be only upto SELECTOR_POLL_MS.
        // But sometimes wallclock time can be higher
        this.selectTimeMsHistogram = new Histogram(SelectorManager.SELECTOR_POLL_MS * 2,
                                                   1,
                                                   SELECTOR_STATS_RESET_INTERVAL);
        // Not a scientific limit. Not expecting a server thread to handle more
        // than 100K connections.
        this.selectCountHistogram = new Histogram(100000, 1, SELECTOR_STATS_RESET_INTERVAL);
        // again not scientific. But we really don't care about any processing
        // time higher than 15 seconds
        this.processingTimeMsHistogram = new Histogram(15000, 1, SELECTOR_STATS_RESET_INTERVAL);
    }

    public void addConnection() {
        numActiveConnections.increment();
    }

    public void removeConnection() {
        numActiveConnections.decrement();
    }

    public void updateSelectStats(int selectCount, long selectTimeMs, long processingTimeMs) {
        // update selection statistics
        if(selectCount > -1) {
            selectCountHistogram.insert(selectCount);
            selectTimeMsHistogram.insert(selectTimeMs);
        }
        // update processing time statistics only if some work was picked up
        if(processingTimeMs > -1 && selectCount > 0) {
            processingTimeMsHistogram.insert(processingTimeMs);
        }
    }

    /**
     * Returns the number of active connections for this selector manager
     * 
     * @return
     */
    public Integer getNumActiveConnections() {
        return numActiveConnections.toInteger();
    }

    public Histogram getSelectTimeMsHistogram() {
        return selectTimeMsHistogram;
    }

    public Histogram getSelectCountHistogram() {
        return selectCountHistogram;
    }

    public Histogram getProcessingTimeMsHistogram() {
        return processingTimeMsHistogram;
    }

    public CommBufferSizeStats getServerCommBufferStats() {
        return serverCommBufferStats;
    }
}
