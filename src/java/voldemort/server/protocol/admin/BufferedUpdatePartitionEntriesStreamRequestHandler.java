package voldemort.server.protocol.admin;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import voldemort.VoldemortException;
import voldemort.client.protocol.pb.VAdminProto.UpdatePartitionEntriesRequest;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StorageEngine;
import voldemort.store.StoreUtils;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.NetworkClassLoader;
import voldemort.versioning.Versioned;

/**
 * The buffering is so that if we the stream contains multiple versions for the
 * same key, then we would want the storage to be updated with all the versions
 * atomically, to make sure client does not read a partial set of versions at
 * any point
 * 
 * TODO with the removal of Donor based rebalancing, this class has little value
 * and should be removed.
 */
class BufferedUpdatePartitionEntriesStreamRequestHandler extends
        UpdatePartitionEntriesStreamRequestHandler {

    private static final int VALS_BUFFER_EXPECTED_SIZE = 5;
    /**
     * Current key being buffered.
     */
    private ByteArray currBufferedKey;

    private List<Versioned<byte[]>> currBufferedVals;

    public BufferedUpdatePartitionEntriesStreamRequestHandler(UpdatePartitionEntriesRequest request,
                                                              ErrorCodeMapper errorCodeMapper,
                                                              VoldemortConfig voldemortConfig,
                                                              StorageEngine<ByteArray, byte[], byte[]> storageEngine,
                                                              StoreRepository storeRepository,
                                                              NetworkClassLoader networkClassLoader,
                                                              MetadataStore metadataStore) {
        super(request,
              errorCodeMapper,
              voldemortConfig,
              storageEngine,
              storeRepository,
              networkClassLoader,
              metadataStore);
        currBufferedKey = null;
        currBufferedVals = new ArrayList<Versioned<byte[]>>(VALS_BUFFER_EXPECTED_SIZE);
    }

    /**
     * Persists the current set of versions buffered for the current key into
     * storage, using the multiVersionPut api
     * 
     * NOTE: Now, it could be that the stream broke off and has more pending
     * versions. For now, we simply commit what we have to disk. A better design
     * would rely on in-stream markers to do the flushing to storage.
     */
    private void writeBufferedValsToStorage() {
        List<Versioned<byte[]>> obsoleteVals = storageEngine.multiVersionPut(currBufferedKey,
                                                                             currBufferedVals);
        // log Obsolete versions in debug mode
        if(logger.isDebugEnabled() && obsoleteVals.size() > 0) {
            logger.debug("updateEntries (Streaming multi-version-put) rejected these versions as obsolete : "
                         + StoreUtils.getVersions(obsoleteVals) + " for key " + currBufferedKey);
        }
        currBufferedVals = new ArrayList<Versioned<byte[]>>(VALS_BUFFER_EXPECTED_SIZE);
    }

    private void writeBufferedValsToStorageIfAny() {
        if(currBufferedVals.size() > 0) {
            writeBufferedValsToStorage();
        }
    }

    @Override
    public void close(DataOutputStream outputStream) throws IOException {
        writeBufferedValsToStorageIfAny();
        super.close(outputStream);
    }

    @Override
    public void handleError(DataOutputStream outputStream, VoldemortException e) throws IOException {
        writeBufferedValsToStorageIfAny();
        super.handleError(outputStream, e);
    }

    @Override
    protected void finalize() {
        super.finalize();
        /*
         * Also check if we have any pending values being buffered. if so, flush
         * to storage.
         */
        writeBufferedValsToStorageIfAny();
    }

    @Override
    protected void processEntry(ByteArray key, Versioned<byte[]> value) throws IOException {
        // Check if the current key is same as the one before.
        if(currBufferedKey != null && !key.equals(currBufferedKey)) {
            // if not, write buffered values for the previous key to storage
            writeBufferedValsToStorage();
        }
        currBufferedKey = key;
        currBufferedVals.add(value);
    }

    @Override
    protected String getHandlerName() {
        return "BufferedUpdateEntries";
    }
}
