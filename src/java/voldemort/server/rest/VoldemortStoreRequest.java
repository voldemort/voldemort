package voldemort.server.rest;

import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

public class VoldemortStoreRequest {

    private final CompositeVoldemortRequest<ByteArray, byte[]> requestObject;
    private final Store<ByteArray, byte[], byte[]> store;

    public VoldemortStoreRequest(CompositeVoldemortRequest<ByteArray, byte[]> requestObject,
                                       Store<ByteArray, byte[], byte[]> store) {
        this.requestObject = requestObject;
        this.store = store;
    }

    public CompositeVoldemortRequest<ByteArray, byte[]> getRequestObject() {
        return requestObject;
    }

    public Store<ByteArray, byte[], byte[]> getStore() {
        return store;
    }

}
