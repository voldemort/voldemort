package voldemort.performance.benchmark;

import voldemort.client.StoreClient;
import voldemort.utils.Props;

/**
 * This interface defines the methods that will be called by {@link Benchmark}.
 */
public interface BenchmarkWorkload {

    public boolean doTransaction(VoldemortWrapper db, WorkloadPlugin plugin);

    public boolean doWrite(VoldemortWrapper db, WorkloadPlugin plugin);

    public void init(Props props);

    public void loadSampleValues(StoreClient<Object, Object> client);
}
