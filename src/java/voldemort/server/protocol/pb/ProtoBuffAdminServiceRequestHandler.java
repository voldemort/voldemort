package voldemort.server.protocol.pb;


import com.google.protobuf.Message;
import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.client.protocol.admin.filter.DefaultVoldemortFilter;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.VoldemortAdminRequest;
import voldemort.routing.RoutingStrategy;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.RequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.*;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;


/**
 * Created by IntelliJ IDEA.
 * User: afeinber
 * Date: Sep 29, 2009
 * Time: 4:45:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class ProtoBuffAdminServiceRequestHandler implements RequestHandler {
    private final Logger logger = Logger.getLogger(ProtoBuffAdminServiceRequestHandler.class);

    private final ErrorCodeMapper errorCodeMapper;
    private final MetadataStore metadataStore;
    private final StoreRepository storeRepository;
    private final NetworkClassLoader networkClassLoader;
    private final int streamMaxBytesReadPerSec;
    private final int streamMaxBytesWritesPerSec;

    
    public ProtoBuffAdminServiceRequestHandler(ErrorCodeMapper errorCodeMapper,
                                               StoreRepository storeRepository,
                                               MetadataStore metadataStore,
                                               int streamMaxBytesReadPerSec,
                                               int streamMaxBytesWritesPerSec) {
        this.errorCodeMapper = errorCodeMapper;
        this.metadataStore = metadataStore;
        this.storeRepository = storeRepository;

        this.streamMaxBytesReadPerSec = streamMaxBytesReadPerSec;
        this.streamMaxBytesWritesPerSec = streamMaxBytesWritesPerSec;
        
        this.networkClassLoader = new NetworkClassLoader(Thread.currentThread()
                .getContextClassLoader());
    }
    
    //@Override
    public void handleRequest(DataInputStream inputStream, DataOutputStream outputStream) throws IOException {
        VoldemortAdminRequest.Builder request = ProtoUtils.readToBuilder(inputStream,
                VoldemortAdminRequest.newBuilder());
        Message response;

        switch(request.getType()) {
            case GET_METADATA:
                response = handleGetMetadata(request.getGetMetadata());
                break;
            case UPDATE_METADATA:
                response = handleUpdateMetadata(request.getUpdateMetadata());
                break;
            case DELETE_PARTITION_ENTRIES:
                response = handleDeletePartitionEntries(request.getDeletePartitionEntries());
                break;
            case FETCH_PARTITION_ENTRIES:
                response = handleFetchPartitionEntries(request.getFetchPartitionEntries())
                break;
            case REDIRECT_GET:
                response = handleRedirectGet(request.getRedirectGet());
                break;
          //  case UPDATE_PARTITION_ENTRIES: break;
            default:
                throw new VoldemortException("Unkown operation " + request.getType());
        }
        ProtoUtils.writeMessage(outputStream, response);
        outputStream.flush();
    }

    private VoldemortFilter getFilterFromRequest(VAdminProto.VoldemortFilter request) {
        VoldemortFilter filter;
        byte[] classBytes = ProtoUtils.decodeBytes(request.getData())
                .get();
        String className = request.getName();
        try {
            Class<?> cl = networkClassLoader.loadClass(className, classBytes, 0, classBytes.length);
            filter = (VoldemortFilter) cl.newInstance();
        } catch (Exception e) {
            throw new VoldemortException("Failed to load and instantiate the filter class", e);
        }

        return filter;
    }

    public VAdminProto.RedirectGetResponse handleRedirectGet(VAdminProto.RedirectGetRequest request) {
        VAdminProto.RedirectGetResponse.Builder response =
                VAdminProto.RedirectGetResponse.newBuilder();
        try {
            String storeName = request.getStoreName() ;
            ByteArray key = ProtoUtils.decodeBytes(request.getKey());
            StorageEngine<ByteArray, byte[]> storageEngine =
                    storeRepository.getStorageEngine(storeName);
            if (storageEngine == null) {
                throw new VoldemortException("No stored named '" + storeName + "'.");
            }
            List<Versioned<byte[]>> results = storageEngine.get(key);
            for (Versioned<byte[]> result: results) {
                response.addVersioned(ProtoUtils.encodeVersioned(result));
            }
        } catch (VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
        }
        return response.build();
    }


    public VAdminProto.FetchPartitionEntriesResponse handleFetchPartitionEntries
            (VAdminProto.FetchPartitionEntriesRequest request) {
        VAdminProto.FetchPartitionEntriesResponse.Builder response =
                VAdminProto.FetchPartitionEntriesResponse.newBuilder();
        try {
            String storeName = request.getStore();
            if (request.hasStart()) {
                // Start the fetch partition entries request
            } else {
                // Continue the fetch partition entries request if one is started
            }

        } catch (VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
        }

        return response.build();
    }
    public VAdminProto.DeletePartitionEntriesResponse
    handleDeletePartitionEntries(VAdminProto.DeletePartitionEntriesRequest request) {
        VAdminProto.DeletePartitionEntriesResponse.Builder response =
                VAdminProto.DeletePartitionEntriesResponse.newBuilder();
        try {
            String storeName = request.getStore();
            List<Integer> partitions = request.getPartitionsList();
            StorageEngine<ByteArray, byte[]> storageEngine = storeRepository.getStorageEngine(storeName);
            if (storageEngine == null) {
                throw new VoldemortException("No store named '" + storeName + "'.");
            }
            RoutingStrategy routingStrategy = metadataStore.getRoutingStrategy(storageEngine.getName());
            VoldemortFilter filter = request.hasFilter() ? getFilterFromRequest(request.getFilter()) :
                    new DefaultVoldemortFilter();
            IoThrottler throttler = new IoThrottler(streamMaxBytesReadPerSec);

            ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iterator = storageEngine.entries();

            int deleteSuccess = 0;
            while (iterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> entry = iterator.next();

                if (validPartition(entry.getFirst().get(), partitions, routingStrategy) &&
                        filter.filter(entry.getFirst(), entry.getSecond())) {
                    if (storageEngine.delete(entry.getFirst(), entry.getSecond().getVersion()))
                        deleteSuccess++;

                    if (throttler != null)
                        throttler.maybeThrottle(entry.getFirst().get().length +
                                entry.getSecond().getValue().length +
                                ((VectorClock) entry.getSecond().getVersion()).sizeInBytes());
                }

            }

            iterator.close();
            response.setCount(deleteSuccess);
        } catch (VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
        }
        
        return response.build();
    }

    public VAdminProto.UpdateMetadataResponse handleUpdateMetadata(VAdminProto.UpdateMetadataRequest request) {
        VAdminProto.UpdateMetadataResponse.Builder response = VAdminProto.UpdateMetadataResponse.newBuilder();

        try {
            ByteArray key = ProtoUtils.decodeBytes(request.getKey());
            String keyString = ByteUtils.getString(key.get(), "UTF-8");

            if (MetadataStore.METADATA_KEYS.contains(keyString)) {
                Versioned<byte[]> versionedValue = ProtoUtils.decodeVersioned(request.getVersioned());
                metadataStore.put(new ByteArray(ByteUtils.getBytes(keyString, "UTF-8")), versionedValue);
            }
            

        } catch (VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
        }

        return response.build();
    }

    public VAdminProto.GetMetadataResponse handleGetMetadata(VAdminProto.GetMetadataRequest request) {
        VAdminProto.GetMetadataResponse.Builder response = VAdminProto.GetMetadataResponse.newBuilder();

        try {
            ByteArray key = ProtoUtils.decodeBytes(request.getKey());
            String keyString = ByteUtils.getString(key.get(), "UTF-8");

            if (MetadataStore.METADATA_KEYS.contains(keyString)) {
                List<Versioned<byte[]>> versionedList =
                        metadataStore.get(key);
                int size = (versionedList.size() > 0) ? 1 : 0;
                if (size > 0) {
                    Versioned<byte[]> versioned = versionedList.get(0);
                    response.setVersion(ProtoUtils.encodeVersioned(versioned));
                }
            } else {
                throw new VoldemortException("Metadata Key passed " + keyString + " is not handled yet ...");

            }
        } catch (VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
        }
        return response.build();

    }

    protected boolean validPartition(byte[] key,
                                     List<Integer> partitionList,
                                     RoutingStrategy routingStrategy) {
        List<Integer> keyPartitions = routingStrategy.getPartitionList(key);
        for(int p: partitionList) {
            if(keyPartitions.contains(p)) {
                return true;
            }
        }
        return false;
    }

    /**
     * This method is used by non-blocking code to determine if the give buffer
     * represents a complete request. Because the non-blocking code can by
     * definition not just block waiting for more data, it's possible to get
     * partial reads, and this identifies that case.
     *
     * @param buffer Buffer to check; the buffer is reset to position 0 before
     *               calling this method and the caller must reset it after the call
     *               returns
     * @return True if the buffer holds a complete request, false otherwise
     */
    //@Override
    public boolean isCompleteRequest(ByteBuffer buffer) {
        throw new VoldemortException("Non-blocking server not supported for ProtoBuffAdminServiceRequestHandler");
    }
}
