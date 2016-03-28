package voldemort.store.readonly.fetcher;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.mockito.Mockito;

public class HdfsFetchAggStatsTest {
    @Test
    public void testSingleFileFetchStartWithoutRetry() {
        long parallelFetchesBefore = HdfsFetcherAggStats.getStats().getParallelFetches();
        long totalFetchRetiesBefore = HdfsFetcherAggStats.getStats().getTotalFetchRetries();
        HdfsFetcherAggStats.getStats().singleFileFetchStart(false);

        Assert.assertEquals(parallelFetchesBefore + 1, HdfsFetcherAggStats.getStats().getParallelFetches());
        Assert.assertEquals(totalFetchRetiesBefore, HdfsFetcherAggStats.getStats().getTotalFetchRetries());
    }

    @Test
    public void testSingleFileFetchStartWithRetry() {
        long parallelFetchesBefore = HdfsFetcherAggStats.getStats().getParallelFetches();
        long totalFetchRetiesBefore = HdfsFetcherAggStats.getStats().getTotalFetchRetries();
        HdfsFetcherAggStats.getStats().singleFileFetchStart(true);

        Assert.assertEquals(parallelFetchesBefore + 1, HdfsFetcherAggStats.getStats().getParallelFetches());
        Assert.assertEquals(totalFetchRetiesBefore + 1, HdfsFetcherAggStats.getStats().getTotalFetchRetries());
    }

    @Test
    public void testGetTotalDataFetchRateWithoutStorePush() {
        synchronized (HdfsFetcherAggStats.class) {
            Assert.assertEquals(0, HdfsFetcherAggStats.getStats().getTotalDataFetchRate(), 0);
        }
    }

    @Test
    public void testGetTotalDataFetchRateWithTwoStorePushes() {
        // Mock two HdfsCopyStats objects
        String testStore1 = "test1";
        HdfsCopyStats stats1 = Mockito.mock(HdfsCopyStats.class);
        BDDMockito.given(stats1.getBytesTransferredPerSecond()).willReturn(100.2);

        String testStore2 = "test2";
        HdfsCopyStats stats2 = Mockito.mock(HdfsCopyStats.class);
        BDDMockito.given(stats2.getBytesTransferredPerSecond()).willReturn(50.3);

        // Add them into aggregated stats object
        HdfsFetcherAggStats.getStats().addStoreCopyStats(testStore1, stats1);
        HdfsFetcherAggStats.getStats().addStoreCopyStats(testStore2, stats2);

        double totalDataFetchRate = HdfsFetcherAggStats.getStats().getTotalDataFetchRate();

        // Remove them to avoid side effect
        HdfsFetcherAggStats.getStats().removeStoreCopyStats(testStore1);
        HdfsFetcherAggStats.getStats().removeStoreCopyStats(testStore2);

        Assert.assertEquals(150.5, totalDataFetchRate, 0);
        // After removal, the data transfer rate should be 0
        Assert.assertEquals(0, HdfsFetcherAggStats.getStats().getTotalDataFetchRate(), 0);

    }
}
