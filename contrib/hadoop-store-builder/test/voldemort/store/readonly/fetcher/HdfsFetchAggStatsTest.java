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
}
