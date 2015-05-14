package voldemort.server.protocol.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import voldemort.client.protocol.pb.VAdminProto.UpdatePartitionEntriesRequest;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.storage.KeyLockHandle;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.NetworkClassLoader;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * This class implements a streaming interface into a voldemort server, that
 * resolves based on timestamp.
 * 
 * Specifically, the policy implemented is "overwrite if later". That is, if
 * (and only if), the streamed in version's timestamp is greater than the
 * timestamps of ALL" versions on storage, the key is overwritten atomically
 * with the streamed version. Else, the key is unmodified.
 * 
 * NOTE: This class does not do any buffering like
 * {@link BufferedUpdatePartitionEntriesStreamRequestHandler} since this is
 * mainly intended to be used for streaming values in from external sources.
 * 
 */
public class TimeBasedUpdatePartitionEntriesStreamRequestHandler extends
        UpdatePartitionEntriesStreamRequestHandler {

    public TimeBasedUpdatePartitionEntriesStreamRequestHandler(UpdatePartitionEntriesRequest request,
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
    }

    @Override
    protected void processEntry(ByteArray key, Versioned<byte[]> value) throws IOException {
        KeyLockHandle<byte[]> handle = null;
        try {
            handle = storageEngine.getAndLock(key);

            // determine if there is a version already with a greater ts.
            boolean foundGreaterTs = false;
            VectorClock streamedClock = (VectorClock) value.getVersion();
            for(Versioned<byte[]> versioned: handle.getValues()) {
                VectorClock storedClock = (VectorClock) versioned.getVersion();
                if(storedClock.getTimestamp() >= streamedClock.getTimestamp()) {
                    foundGreaterTs = true;
                    break;
                }
            }

            if(!foundGreaterTs) {
                // if what we are trying to write is the greatest version,
                // write it in and let go of lock
                List<Versioned<byte[]>> streamedVals = new ArrayList<Versioned<byte[]>>(1);
                streamedVals.add(value);
                handle.setValues(streamedVals);
                storageEngine.putAndUnlock(key, handle);
            } else {
                // back off and let go of lock, if found a version that is
                // greater than what we are trying to write in
                storageEngine.releaseLock(handle);
            }
        } catch(Exception e) {
            logger.error("Error in time based update entries", e);
            throw new IOException(e);
        } finally {
            if(handle != null && !handle.isClosed()) {
                storageEngine.releaseLock(handle);
            }
        }
    }

    @Override
    protected String getHandlerName() {
        return "TimeBasedUpdateEntries";
    }
}
