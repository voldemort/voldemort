package voldemort.server.protocol.pb;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VProto;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.AbstractRequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import com.google.protobuf.ByteString;

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
        VProto.VoldemortRequest request = VProto.VoldemortRequest.parseFrom(inputStream);
        boolean shouldRoute = request.getShouldRoute();
        String storeName = request.getStore();
        Store<ByteArray, byte[]> store = getStore(storeName, shouldRoute);
        switch(request.getType()) {
            case GET:
                handleGet(request.getGet(), store, outputStream);
                break;
            case GET_ALL:
                handleGetAll(request.getGetAll(), store, outputStream);
                break;
            case PUT:
                handlePut(request.getPut(), store, outputStream);
                break;
            case DELETE:
                handleDelete(request.getDelete(), store, outputStream);
                break;
        }
    }

    private void handleGet(VProto.GetRequest request,
                           Store<ByteArray, byte[]> store,
                           DataOutputStream outputStream) throws IOException {
        VProto.GetResponse.Builder response = VProto.GetResponse.newBuilder();
        try {
            List<Versioned<byte[]>> values = store.get(ProtoUtils.decodeBytes(request.getKey()));
            for(Versioned<byte[]> versioned: values)
                response.addVersioned(ProtoUtils.encodeVersioned(versioned));
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(getErrorMapper(), e));
        }
        response.build().writeTo(outputStream);
    }

    private void handleGetAll(VProto.GetAllRequest request,
                              Store<ByteArray, byte[]> store,
                              DataOutputStream outputStream) throws IOException {
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
        response.build().writeTo(outputStream);
    }

    private void handlePut(VProto.PutRequest request,
                           Store<ByteArray, byte[]> store,
                           DataOutputStream outputStream) throws IOException {
        VProto.PutResponse.Builder response = VProto.PutResponse.newBuilder();
        try {
            ByteArray key = ProtoUtils.decodeBytes(request.getKey());
            Versioned<byte[]> value = ProtoUtils.decodeVersioned(request.getVersioned());
            store.put(key, value);
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(getErrorMapper(), e));
        }
        response.build().writeTo(outputStream);
    }

    private void handleDelete(VProto.DeleteRequest request,
                              Store<ByteArray, byte[]> store,
                              DataOutputStream outputStream) throws IOException {
        VProto.DeleteResponse.Builder response = VProto.DeleteResponse.newBuilder();
        try {
            boolean success = store.delete(ProtoUtils.decodeBytes(request.getKey()),
                                           ProtoUtils.decodeClock(request.getVersion()));
            response.setSuccess(success);
        } catch(VoldemortException e) {
            response.setSuccess(false);
            response.setError(ProtoUtils.encodeError(getErrorMapper(), e));
        }
        response.build().writeTo(outputStream);
    }

}
