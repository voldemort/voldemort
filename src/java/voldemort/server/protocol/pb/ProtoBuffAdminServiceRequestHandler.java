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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;


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
    private final int windowSize;


    public ProtoBuffAdminServiceRequestHandler(ErrorCodeMapper errorCodeMapper,
                                               StoreRepository storeRepository,
                                               MetadataStore metadataStore,
                                               int streamMaxBytesReadPerSec,
                                               int streamMaxBytesWritesPerSec) {
        this(errorCodeMapper, storeRepository, metadataStore, streamMaxBytesReadPerSec,
                streamMaxBytesWritesPerSec, 10);
    }

    public ProtoBuffAdminServiceRequestHandler(ErrorCodeMapper errorCodeMapper,
                                               StoreRepository storeRepository,
                                               MetadataStore metadataStore,
                                               int streamMaxBytesReadPerSec,
                                               int streamMaxBytesWritesPerSec,
                                               int windowSize) {
        this.errorCodeMapper = errorCodeMapper;
        this.metadataStore = metadataStore;
        this.storeRepository = storeRepository;

        this.streamMaxBytesReadPerSec = streamMaxBytesReadPerSec;
        this.streamMaxBytesWritesPerSec = streamMaxBytesWritesPerSec;
        
        this.networkClassLoader = new NetworkClassLoader(Thread.currentThread()
                .getContextClassLoader());
        this.windowSize = windowSize;
    }
    
    //@Override
    public void handleRequest(final DataInputStream inputStream, final DataOutputStream outputStream) throws IOException {
        final VoldemortAdminRequest.Builder request = ProtoUtils.readToBuilder(inputStream,
                VoldemortAdminRequest.newBuilder());

        switch(request.getType()) {
            case GET_METADATA:
                writeMessageAndFlush(handleGetMetadata(request.getGetMetadata()), outputStream);
                break;
            case UPDATE_METADATA:
                writeMessageAndFlush(handleUpdateMetadata(request.getUpdateMetadata()), outputStream);
                break;
            case DELETE_PARTITION_ENTRIES:
                writeMessageAndFlush(handleDeletePartitionEntries(request.getDeletePartitionEntries()),
                        outputStream);
                break;
            case FETCH_PARTITION_ENTRIES:
//
                handleFetchPartitionEntries(request.getFetchPartitionEntries(), outputStream);
//
                break;
            case REDIRECT_GET:
                writeMessageAndFlush(handleRedirectGet(request.getRedirectGet()), outputStream);
                break;
            case UPDATE_PARTITION_ENTRIES:

                handleUpdatePartitionEntries(request.getUpdatePartitionEntries(), inputStream, outputStream);

                break;
            default:
                throw new VoldemortException("Unkown operation " + request.getType());
        }

    }

    private void writeMessageAndFlush(Message response, DataOutputStream outputStream) throws IOException {
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

    public int writeWindowToStream(Queue<Pair<ByteArray, Versioned<byte[]>>> window, DataOutputStream outputStream,
                                   boolean lastMessage)
            throws IOException
    {
        VAdminProto.FetchPartitionEntriesResponse.Builder
                responseBuilder = VAdminProto.FetchPartitionEntriesResponse.newBuilder();
        while (!window.isEmpty()) {
            VAdminProto.PartitionEntry.Builder partitionEntry =
                    VAdminProto.PartitionEntry.newBuilder();

            Pair<ByteArray, Versioned<byte[]>> queuedEntry =
                    window.remove();

            partitionEntry.setKey(ProtoUtils.encodeBytes(queuedEntry.getFirst()));
            partitionEntry.setVersioned(ProtoUtils.encodeVersioned(queuedEntry.getSecond()));

            responseBuilder.addPartitionEntries(partitionEntry);
        }
        responseBuilder.setContinue(!lastMessage);
        Message response = responseBuilder.build();
        writeMessageAndFlush(response, outputStream);
        return response.getSerializedSize();
    }

    public void handleFetchPartitionEntries(VAdminProto.FetchPartitionEntriesRequest request,
                                            DataOutputStream outputStream) throws IOException {
        try {
            String storeName = request.getStore();
            StorageEngine<ByteArray, byte[]> storageEngine = storeRepository.getStorageEngine(storeName);
            if (storageEngine == null) {
                throw new VoldemortException("No store named '" + storeName + "'.");
            }
            RoutingStrategy routingStrategy = metadataStore.getRoutingStrategy(storageEngine.getName());
            IoThrottler throttler = new IoThrottler(streamMaxBytesReadPerSec);
            List<Integer> partitionList = request.getPartitionsList();

            VoldemortFilter filter;
            if (request.hasFilter()) {
                filter = getFilterFromRequest(request.getFilter());
            } else {
                filter = new DefaultVoldemortFilter();
            }

            ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iterator = storageEngine.entries();
            Queue<Pair<ByteArray, Versioned<byte[]>>> window = new
                    LinkedList<Pair<ByteArray, Versioned<byte[]>>>();
            while (iterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> entry = iterator.next();

                if (validPartition(entry.getFirst().get(), partitionList, routingStrategy)
                    && filter.filter(entry.getFirst(), entry.getSecond())) {
                    window.add(entry);
                    if (window.size() >= windowSize) {
                        writeWindowToStream(window, outputStream, !iterator.hasNext());
                        if (throttler != null) {
                           throttler.maybeThrottle(entry.getFirst().length() +
                                   entry.getSecond().getValue().length + 1);
                        }
                    }
                }
            }
            if (!window.isEmpty())
                writeWindowToStream(window, outputStream, true);

        } catch (VoldemortException e) {
            VAdminProto.FetchPartitionEntriesResponse.Builder response =
                    VAdminProto.FetchPartitionEntriesResponse.newBuilder();
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            response.setContinue(false);
            writeMessageAndFlush(response.build(), outputStream);
        }
    }

    public void handleUpdatePartitionEntries(VAdminProto.UpdatePartitionEntriesRequest originalRequest,
                                             DataInputStream inputStream, DataOutputStream outputStream)
            throws IOException {
        VAdminProto.UpdatePartitionEntriesRequest.Builder request = originalRequest.toBuilder();
        boolean continueFetching=true;
        boolean firstFetched=true;
        try {
            String storeName = request.getStore();
            StorageEngine<ByteArray, byte[]> storageEngine = storeRepository.getStorageEngine(storeName);
            if (storageEngine == null) {
                throw new VoldemortException("No stored named '" + storeName + "'.");
            }
            IoThrottler throttler = new IoThrottler(streamMaxBytesWritesPerSec);
            while (continueFetching) {
                if (!firstFetched) {
                    request = ProtoUtils.readToBuilder(inputStream, VAdminProto.UpdatePartitionEntriesRequest
                            .newBuilder());
                } else {
                    firstFetched = false;
                }
                for (VAdminProto.PartitionEntry partitionEntry: request.getPartitionEntriesList()) {
                    ByteArray key = ProtoUtils.decodeBytes(partitionEntry.getKey());
                    Versioned<byte[]> value = ProtoUtils.decodeVersioned(partitionEntry.getVersioned());
                    storageEngine.put(key, value);
                    if (throttler != null) {
                        throttler.maybeThrottle(partitionEntry.getKey().size() +
                                partitionEntry.getVersioned().getValue().size() + 1);
                    }
                }
                continueFetching = request.getContinue();
            }
            
            VAdminProto.UpdatePartitionEntriesResponse response =
                    VAdminProto.UpdatePartitionEntriesResponse.newBuilder().build();
            writeMessageAndFlush(response, outputStream);
        } catch (VoldemortException e) {
            VAdminProto.UpdatePartitionEntriesResponse response =
                    VAdminProto.UpdatePartitionEntriesResponse.newBuilder()
                    .setError(ProtoUtils.encodeError(errorCodeMapper, e))
                    .build();
            writeMessageAndFlush(response, outputStream);
        }
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
