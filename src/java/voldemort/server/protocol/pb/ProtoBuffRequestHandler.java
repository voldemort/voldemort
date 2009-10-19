package voldemort.server.protocol.pb;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VProto;
import voldemort.client.protocol.pb.VProto.GetRequest;
import voldemort.client.protocol.pb.VProto.RequestType;
import voldemort.client.protocol.pb.VProto.VoldemortRequest;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.AbstractRequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

/**
 * A Protocol Buffers request handler
 * 
 * @author jay
 * 
 */
public class ProtoBuffRequestHandler extends AbstractRequestHandler {

    public ProtoBuffRequestHandler(ErrorCodeMapper errorMapper, StoreRepository storeRepository) {
        super(errorMapper, storeRepository);
    }

    public void handleRequest(DataInputStream inputStream, DataOutputStream outputStream)
            throws IOException {
        VoldemortRequest.Builder request = ProtoUtils.readToBuilder(inputStream,
                                                                    VoldemortRequest.newBuilder());
        boolean shouldRoute = request.getShouldRoute();
        String storeName = request.getStore();
        Store<ByteArray, byte[]> store = getStore(storeName, shouldRoute);
        Message response;
        if(store == null) {
            response = unknownStore(storeName, request.getType());
        } else {
            switch(request.getType()) {
                case GET:
                    response = handleGet(request.getGet(), store);
                    break;
                case GET_ALL:
                    response = handleGetAll(request.getGetAll(), store);
                    break;
                case PUT:
                    response = handlePut(request.getPut(), store);
                    break;
                case DELETE:
                    response = handleDelete(request.getDelete(), store);
                    break;
                case GET_VERSION:
                    response = handleGetVersion(request.getGet(), store);
                    break;
                default:
                    throw new VoldemortException("Unknown operation " + request.getType());
            }
        }
        ProtoUtils.writeMessage(outputStream, response);
    }

    private Message handleGetVersion(GetRequest request, Store<ByteArray, byte[]> store) {
        VProto.GetVersionResponse.Builder response = VProto.GetVersionResponse.newBuilder();
        try {
            List<Version> versions = store.getVersions(ProtoUtils.decodeBytes(request.getKey()));
            for(Version version: versions)
                response.addVersions(ProtoUtils.encodeClock(version));
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(getErrorMapper(), e));
        }
        return response.build();
    }

    public boolean isCompleteRequest(ByteBuffer buffer) {
        if(buffer.remaining() < 4)
            return false;

        int size = buffer.getInt();
        return buffer.remaining() == size;
    }

    private VProto.GetResponse handleGet(VProto.GetRequest request, Store<ByteArray, byte[]> store) {
        VProto.GetResponse.Builder response = VProto.GetResponse.newBuilder();
        try {
            List<Versioned<byte[]>> values = store.get(ProtoUtils.decodeBytes(request.getKey()));
            for(Versioned<byte[]> versioned: values)
                response.addVersioned(ProtoUtils.encodeVersioned(versioned));
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(getErrorMapper(), e));
        }
        return response.build();
    }

    private VProto.GetAllResponse handleGetAll(VProto.GetAllRequest request,
                                               Store<ByteArray, byte[]> store) {
        VProto.GetAllResponse.Builder response = VProto.GetAllResponse.newBuilder();
        try {
            List<ByteArray> keys = new ArrayList<ByteArray>(request.getKeysCount());
            for(ByteString string: request.getKeysList())
                keys.add(ProtoUtils.decodeBytes(string));
            Map<ByteArray, List<Versioned<byte[]>>> values = store.getAll(keys);
            for(Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: values.entrySet()) {
                VProto.KeyedVersions.Builder keyedVersion = VProto.KeyedVersions.newBuilder()
                                                                                .setKey(ProtoUtils.encodeBytes(entry.getKey()));
                for(Versioned<byte[]> version: entry.getValue())
                    keyedVersion.addVersions(ProtoUtils.encodeVersioned(version));
                response.addValues(keyedVersion);
            }
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(getErrorMapper(), e));
        }
        return response.build();
    }

    private VProto.PutResponse handlePut(VProto.PutRequest request, Store<ByteArray, byte[]> store) {
        VProto.PutResponse.Builder response = VProto.PutResponse.newBuilder();
        try {
            ByteArray key = ProtoUtils.decodeBytes(request.getKey());
            Versioned<byte[]> value = ProtoUtils.decodeVersioned(request.getVersioned());
            store.put(key, value);
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(getErrorMapper(), e));
        }
        return response.build();
    }

    private VProto.DeleteResponse handleDelete(VProto.DeleteRequest request,
                                               Store<ByteArray, byte[]> store) {
        VProto.DeleteResponse.Builder response = VProto.DeleteResponse.newBuilder();
        try {
            boolean success = store.delete(ProtoUtils.decodeBytes(request.getKey()),
                                           ProtoUtils.decodeClock(request.getVersion()));
            response.setSuccess(success);
        } catch(VoldemortException e) {
            response.setSuccess(false);
            response.setError(ProtoUtils.encodeError(getErrorMapper(), e));
        }
        return response.build();
    }

    public Message unknownStore(String storeName, RequestType type) {
        VProto.Error error = VProto.Error.newBuilder()
                                         .setErrorCode(getErrorMapper().getCode(VoldemortException.class))
                                         .setErrorMessage("Unknown store '" + storeName + "'.")
                                         .build();
        switch(type) {
            case GET:
                return VProto.GetResponse.newBuilder().setError(error).build();
            case GET_ALL:
                return VProto.GetAllResponse.newBuilder().setError(error).build();
            case PUT:
                return VProto.PutResponse.newBuilder().setError(error).build();
            case DELETE:
                return VProto.DeleteResponse.newBuilder().setError(error).setSuccess(false).build();
            default:
                throw new VoldemortException("Unknown operation " + type);
        }
    }

}
