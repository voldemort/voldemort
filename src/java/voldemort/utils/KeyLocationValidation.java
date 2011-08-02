package voldemort.utils;

import java.util.List;

import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.server.RequestRoutingType;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.versioning.Versioned;

public class KeyLocationValidation {

    public KeyLocationValidation(Cluster cluster,
                                 int nodeId,
                                 StoreDefinition storeDef,
                                 ByteArray keyList) {
        this.nodeId = nodeId;
        this.keyList = keyList;
        this.cluster = cluster;
        this.storeDef = storeDef;
    }

    private int nodeId;
    private ByteArray keyList;
    private Cluster cluster;
    private StoreDefinition storeDef;

    /*
     * Validate location of the 'keyList'
     * 
     * @param testType Indicates how to validate True: Positive test (the keys
     * should be present on nodeId). False : Negative test (the keys should not
     * be present on nodeId)
     */
    public boolean validate(boolean testType) {
        SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                              10000,
                                                                              100000,
                                                                              32 * 1024);
        // Cache connections to all nodes for this store in advance
        Store<ByteArray, byte[], byte[]> socketStore = socketStoreFactory.create(storeDef.getName(),
                                                                                 cluster.getNodeById(nodeId)
                                                                                        .getHost(),
                                                                                 cluster.getNodeById(nodeId)
                                                                                        .getSocketPort(),
                                                                                 RequestFormatType.PROTOCOL_BUFFERS,
                                                                                 RequestRoutingType.IGNORE_CHECKS);
        List<Versioned<byte[]>> value = socketStore.get(keyList, null);

        if(testType == false && (value == null || value.size() == 0)) {
            return true;
        } else if(value != null && value.size() != 0) {
            return true;
        }

        return false;
    }
}
