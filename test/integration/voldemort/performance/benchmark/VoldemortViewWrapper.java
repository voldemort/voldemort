package voldemort.performance.benchmark;

import voldemort.client.ViewStoreClient;
import voldemort.client.ViewUpdateAction;
import voldemort.utils.Time;
import voldemort.versioning.Versioned;

/**
 * @author nnarkhed
 * 
 */
public class VoldemortViewWrapper implements DbWrapper {

    public final int Ok = 0;
    public final int Error = -1;

    private ViewStoreClient<Object, Object, Object> voldemortStore;
    private Metrics measurement;
    private boolean verifyReads;
    private boolean ignoreNulls;

    public static final String READS_STRING = "reads";
    public static final String DELETES_STRING = "deletes";
    public static final String WRITES_STRING = "writes";
    public static final String MIXED_STRING = "transactions";

    public VoldemortViewWrapper(ViewStoreClient<Object, Object, Object> storeClient,
                                boolean verifyReads,
                                boolean ignoreNulls) {
        this.voldemortStore = storeClient;
        this.measurement = Metrics.getInstance();
        this.verifyReads = verifyReads;
        this.ignoreNulls = ignoreNulls;
    }

    public int read(Object key, Object expectedValue) {
        long startNs = System.nanoTime();
        Versioned<Object> returnedValue = voldemortStore.get(key);
        long endNs = System.nanoTime();
        measurement.measure(READS_STRING, (int) ((endNs - startNs) / Time.NS_PER_MS));

        int res = this.Ok;
        if(returnedValue == null && !this.ignoreNulls) {
            res = this.Error;
        }

        if(verifyReads && !expectedValue.equals(returnedValue.getValue())) {
            res = this.Error;
        }

        measurement.reportReturnCode(READS_STRING, res);
        return res;
    }

    public int read(Object key, Object expectedValue, Object transforms) {
        long startNs = System.nanoTime();
        Versioned<Object> returnedValue = voldemortStore.get(key, transforms);
        long endNs = System.nanoTime();
        measurement.measure(READS_STRING, (int) ((endNs - startNs) / Time.NS_PER_MS));

        int res = this.Ok;
        if(returnedValue == null && !this.ignoreNulls) {
            res = this.Error;
        }

        if(verifyReads && !expectedValue.equals(returnedValue.getValue())) {
            res = this.Error;
        }

        measurement.reportReturnCode(READS_STRING, res);
        return res;
    }

    public int mixed(final Object key, final Object newValue) {

        boolean updated = voldemortStore.applyUpdate(new ViewUpdateAction<Object, Object, Object>() {

                                                         @Override
                                                         public void update(ViewStoreClient<Object, Object, Object> storeClient) {
                                                             long startNs = System.nanoTime();
                                                             Versioned<Object> v = storeClient.get(key);
                                                             if(v != null) {
                                                                 voldemortStore.put(key, newValue);
                                                             }
                                                             long endNs = System.nanoTime();
                                                             measurement.measure(MIXED_STRING,
                                                                                 (int) ((endNs - startNs) / Time.NS_PER_MS));
                                                         }
                                                     },
                                                     3);

        int res = this.Error;
        if(updated) {
            res = this.Ok;
        }

        measurement.reportReturnCode(MIXED_STRING, res);
        return res;
    }

    public int mixed(final Object key, final Object newValue, final Object transforms) {

        boolean updated = voldemortStore.applyUpdate(new ViewUpdateAction<Object, Object, Object>() {

                                                         @Override
                                                         public void update(ViewStoreClient<Object, Object, Object> storeClient) {
                                                             long startNs = System.nanoTime();
                                                             Versioned<Object> v = storeClient.get(key,
                                                                                                   transforms);
                                                             if(v != null) {
                                                                 voldemortStore.put(key, newValue);
                                                             }
                                                             long endNs = System.nanoTime();
                                                             measurement.measure(MIXED_STRING,
                                                                                 (int) ((endNs - startNs) / Time.NS_PER_MS));
                                                         }
                                                     },
                                                     3);

        int res = this.Error;
        if(updated) {
            res = this.Ok;
        }

        measurement.reportReturnCode(MIXED_STRING, res);
        return res;
    }

    public int write(final Object key, final Object value) {

        boolean written = voldemortStore.applyUpdate(new ViewUpdateAction<Object, Object, Object>() {

                                                         @Override
                                                         public void update(ViewStoreClient<Object, Object, Object> storeClient) {
                                                             long startNs = System.nanoTime();
                                                             storeClient.put(key, value);
                                                             long endNs = System.nanoTime();
                                                             measurement.measure(WRITES_STRING,
                                                                                 (int) ((endNs - startNs) / Time.NS_PER_MS));
                                                         }
                                                     },
                                                     3);

        int res = this.Error;
        if(written) {
            res = this.Ok;
        }

        measurement.reportReturnCode(WRITES_STRING, this.Ok);
        return res;
    }

    public int write(final Object key, final Object value, final Object transforms) {

        boolean written = voldemortStore.applyUpdate(new ViewUpdateAction<Object, Object, Object>() {

                                                         @Override
                                                         public void update(ViewStoreClient<Object, Object, Object> storeClient) {
                                                             long startNs = System.nanoTime();
                                                             storeClient.put(key, value, transforms);
                                                             long endNs = System.nanoTime();
                                                             measurement.measure(WRITES_STRING,
                                                                                 (int) ((endNs - startNs) / Time.NS_PER_MS));
                                                         }
                                                     },
                                                     3);

        int res = this.Error;
        if(written) {
            res = this.Ok;
        }

        measurement.reportReturnCode(WRITES_STRING, this.Ok);
        return res;
    }

    public int delete(Object key) {
        long startNs = System.nanoTime();
        boolean deleted = voldemortStore.delete(key);
        long endNs = System.nanoTime();

        int res = this.Error;
        if(deleted) {
            res = this.Ok;
        }

        measurement.measure(DELETES_STRING, (int) ((endNs - startNs) / Time.NS_PER_MS));
        measurement.reportReturnCode(DELETES_STRING, res);
        return res;
    }

}
