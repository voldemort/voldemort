package voldemort.rest.server;

import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

public class VoldemortStoreRequest {

    private final CompositeVoldemortRequest<ByteArray, byte[]> requestObject;
    private final Store<ByteArray, byte[], byte[]> store;
    private final int zoneId;

    public VoldemortStoreRequest(CompositeVoldemortRequest<ByteArray, byte[]> requestObject,
                                 Store<ByteArray, byte[], byte[]> store,
                                 int zoneId) {
        this.requestObject = requestObject;
        this.store = store;
        this.zoneId = zoneId;
    }

    public CompositeVoldemortRequest<ByteArray, byte[]> getRequestObject() {
        return requestObject;
    }

    public Store<ByteArray, byte[], byte[]> getStore() {
        return store;
    }

    public int getZoneId() {
        return zoneId;
    }
}
