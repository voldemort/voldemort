package voldemort.store.nonblockingstore;

import voldemort.cluster.Node;

public interface NonblockingStoreRequest {

    public void request(Node node, NonblockingStore store);

}
