package voldemort.rest.coordinator;

import voldemort.store.CompositeVoldemortRequest;
import voldemort.utils.ByteArray;

public class CoordinatorStoreClientRequest {

    private final CompositeVoldemortRequest<ByteArray, byte[]> requestObject;
    private final DynamicTimeoutStoreClient<ByteArray, byte[]> storeClient;

    public CoordinatorStoreClientRequest(CompositeVoldemortRequest<ByteArray, byte[]> requestObject,
                                         DynamicTimeoutStoreClient<ByteArray, byte[]> storeClient) {
        this.requestObject = requestObject;
        this.storeClient = storeClient;
    }

    public CompositeVoldemortRequest<ByteArray, byte[]> getRequestObject() {
        return requestObject;
    }

    public DynamicTimeoutStoreClient<ByteArray, byte[]> getStoreClient() {
        return storeClient;
    }

}
