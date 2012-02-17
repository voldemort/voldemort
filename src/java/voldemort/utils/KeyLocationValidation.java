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

    private final int nodeId;
    private final ByteArray keyList;
    private final Cluster cluster;
    private final StoreDefinition storeDef;

    public KeyLocationValidation(Cluster cluster,
                                 int nodeId,
                                 StoreDefinition storeDef,
                                 ByteArray keyList) {
        this.nodeId = nodeId;
        this.keyList = keyList;
        this.cluster = cluster;
        this.storeDef = storeDef;
    }

    /*
     * Validate location of the 'keyList'
     * 
     * @param positiveTest Indicates how to validate True: Positive test (the
     * keys should be present on nodeId). False : Negative test (the keys should
     * not be present on nodeId)
     */
    public boolean validate(boolean positiveTest) {
        boolean retVal = false;

        SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                              10000,
                                                                              100000,
                                                                              32 * 1024);
        // Cache connections to all nodes for this store and given node
        Store<ByteArray, byte[], byte[]> socketStore = socketStoreFactory.create(storeDef.getName(),
                                                                                 cluster.getNodeById(nodeId)
                                                                                        .getHost(),
                                                                                 cluster.getNodeById(nodeId)
                                                                                        .getSocketPort(),
                                                                                 RequestFormatType.PROTOCOL_BUFFERS,
                                                                                 RequestRoutingType.IGNORE_CHECKS);
        List<Versioned<byte[]>> value = socketStore.get(keyList, null);

        if(!positiveTest && (value == null || value.size() == 0)) {
            retVal = true;
        } else if(value != null && value.size() != 0) {
            retVal = true;
        }

        socketStore.close();
        return retVal;
    }
}
