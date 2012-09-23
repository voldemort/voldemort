/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.client.protocol.pb;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.client.protocol.RequestFormat;
import voldemort.client.protocol.pb.VProto.DeleteResponse;
import voldemort.client.protocol.pb.VProto.GetAllResponse;
import voldemort.client.protocol.pb.VProto.GetResponse;
import voldemort.client.protocol.pb.VProto.GetVersionResponse;
import voldemort.client.protocol.pb.VProto.HasKeysResponse;
import voldemort.client.protocol.pb.VProto.PutResponse;
import voldemort.client.protocol.pb.VProto.RequestType;
import voldemort.server.RequestRoutingType;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

/**
 * The client side of the protocol buffers request format
 * 
 * 
 */
public class ProtoBuffClientRequestFormat implements RequestFormat {

    public final ErrorCodeMapper mapper;

    public ProtoBuffClientRequestFormat() {
        this.mapper = new ErrorCodeMapper();
    }

    public void writeDeleteRequest(DataOutputStream output,
                                   String storeName,
                                   ByteArray key,
                                   VectorClock version,
                                   RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        ProtoUtils.writeMessage(output,
                                VProto.VoldemortRequest.newBuilder()
                                                       .setType(RequestType.DELETE)
                                                       .setStore(storeName)
                                                       .setShouldRoute(routingType.equals(RequestRoutingType.ROUTED))
                                                       .setRequestRouteType(routingType.getRoutingTypeCode())
                                                       .setDelete(VProto.DeleteRequest.newBuilder()
                                                                                      .setKey(ByteString.copyFrom(key.get()))
                                                                                      .setVersion(ProtoUtils.encodeClock(version)))
                                                       .build());
    }

    public boolean isCompleteDeleteResponse(ByteBuffer buffer) {
        return isCompleteResponse(buffer);
    }

    public boolean readDeleteResponse(DataInputStream input) throws IOException {
        DeleteResponse.Builder response = ProtoUtils.readToBuilder(input,
                                                                   DeleteResponse.newBuilder());
        if(response.hasError())
            throwException(response.getError());
        return response.getSuccess();
    }

    public void writeGetRequest(DataOutputStream output,
                                String storeName,
                                ByteArray key,
                                byte[] transforms,
                                RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);

        VProto.GetRequest.Builder get = VProto.GetRequest.newBuilder();
        get.setKey(ByteString.copyFrom(key.get()));

        if(transforms != null) {
            get.setTransforms(ByteString.copyFrom(transforms));
        }

        ProtoUtils.writeMessage(output,
                                VProto.VoldemortRequest.newBuilder()
                                                       .setType(RequestType.GET)
                                                       .setStore(storeName)
                                                       .setShouldRoute(routingType.equals(RequestRoutingType.ROUTED))
                                                       .setRequestRouteType(routingType.getRoutingTypeCode())
                                                       .setGet(get)
                                                       .build());
    }

    public boolean isCompleteGetResponse(ByteBuffer buffer) {
        return isCompleteResponse(buffer);
    }

    public List<Versioned<byte[]>> readGetResponse(DataInputStream input) throws IOException {
        GetResponse.Builder response = ProtoUtils.readToBuilder(input, GetResponse.newBuilder());
        if(response.hasError())
            throwException(response.getError());
        return ProtoUtils.decodeVersions(response.getVersionedList());
    }

    public void writeGetAllRequest(DataOutputStream output,
                                   String storeName,
                                   Iterable<ByteArray> keys,
                                   Map<ByteArray, byte[]> transforms,
                                   RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKeys(keys);

        VProto.GetAllRequest.Builder req = VProto.GetAllRequest.newBuilder();
        for(ByteArray key: keys)
            req.addKeys(ByteString.copyFrom(key.get()));

        if(transforms != null) {
            for(Map.Entry<ByteArray, byte[]> transform: transforms.entrySet()) {
                req.addTransforms(VProto.GetAllRequest.GetAllTransform.newBuilder()
                                                                      .setKey(ByteString.copyFrom(transform.getKey()
                                                                                                           .get()))
                                                                      .setTransform(ByteString.copyFrom(transform.getValue())));
            }
        }
        ProtoUtils.writeMessage(output,
                                VProto.VoldemortRequest.newBuilder()
                                                       .setType(RequestType.GET_ALL)
                                                       .setStore(storeName)
                                                       .setShouldRoute(routingType.equals(RequestRoutingType.ROUTED))
                                                       .setRequestRouteType(routingType.getRoutingTypeCode())
                                                       .setGetAll(req)
                                                       .build());
    }

    public boolean isCompleteGetAllResponse(ByteBuffer buffer) {
        return isCompleteResponse(buffer);
    }

    public Map<ByteArray, List<Versioned<byte[]>>> readGetAllResponse(DataInputStream input)
            throws IOException {
        GetAllResponse.Builder response = ProtoUtils.readToBuilder(input,
                                                                   GetAllResponse.newBuilder());
        if(response.hasError())
            throwException(response.getError());
        Map<ByteArray, List<Versioned<byte[]>>> vals = new HashMap<ByteArray, List<Versioned<byte[]>>>(response.getValuesCount());
        for(VProto.KeyedVersions versions: response.getValuesList())
            vals.put(ProtoUtils.decodeBytes(versions.getKey()),
                     ProtoUtils.decodeVersions(versions.getVersionsList()));
        return vals;
    }

    public void writePutRequest(DataOutputStream output,
                                String storeName,
                                ByteArray key,
                                byte[] value,
                                byte[] transforms,
                                VectorClock version,
                                RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        VProto.PutRequest.Builder req = VProto.PutRequest.newBuilder()
                                                         .setKey(ByteString.copyFrom(key.get()))
                                                         .setVersioned(VProto.Versioned.newBuilder()
                                                                                       .setValue(ByteString.copyFrom(value))
                                                                                       .setVersion(ProtoUtils.encodeClock(version)));
        if(transforms != null)
            req = req.setTransforms(ByteString.copyFrom(transforms));

        ProtoUtils.writeMessage(output,
                                VProto.VoldemortRequest.newBuilder()
                                                       .setType(RequestType.PUT)
                                                       .setStore(storeName)
                                                       .setShouldRoute(routingType.equals(RequestRoutingType.ROUTED))
                                                       .setRequestRouteType(routingType.getRoutingTypeCode())
                                                       .setPut(req)
                                                       .build());
    }

    public boolean isCompletePutResponse(ByteBuffer buffer) {
        return isCompleteResponse(buffer);
    }

    public void readPutResponse(DataInputStream input) throws IOException {
        PutResponse.Builder response = ProtoUtils.readToBuilder(input, PutResponse.newBuilder());
        if(response.hasError())
            throwException(response.getError());
    }

    public void throwException(VProto.Error error) {
        throw mapper.getError((short) error.getErrorCode(), error.getErrorMessage());
    }

    public boolean isCompleteGetVersionResponse(ByteBuffer buffer) {
        return isCompleteResponse(buffer);
    }

    public List<Version> readGetVersionResponse(DataInputStream stream) throws IOException {
        GetVersionResponse.Builder response = ProtoUtils.readToBuilder(stream,
                                                                       GetVersionResponse.newBuilder());
        if(response.hasError())
            throwException(response.getError());
        List<Version> versions = Lists.newArrayListWithCapacity(response.getVersionsCount());
        for(VProto.VectorClock version: response.getVersionsList())
            versions.add(ProtoUtils.decodeClock(version));
        return versions;
    }

    public void writeGetVersionRequest(DataOutputStream output,
                                       String storeName,
                                       ByteArray key,
                                       RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        ProtoUtils.writeMessage(output,
                                VProto.VoldemortRequest.newBuilder()
                                                       .setType(RequestType.GET_VERSION)
                                                       .setStore(storeName)
                                                       .setShouldRoute(routingType.equals(RequestRoutingType.ROUTED))
                                                       .setRequestRouteType(routingType.getRoutingTypeCode())
                                                       .setGet(VProto.GetRequest.newBuilder()
                                                                                .setKey(ByteString.copyFrom(key.get())))
                                                       .build());
    }

    private boolean isCompleteResponse(ByteBuffer buffer) {
        int size = buffer.getInt();
        return buffer.remaining() == size;
    }

    public void writeHasKeysRequest(DataOutputStream output,
                                    String storeName,
                                    Iterable<ByteArray> keys,
                                    boolean exact,
                                    RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKeys(keys);

        VProto.HasKeysRequest.Builder req = VProto.HasKeysRequest.newBuilder();
        for(ByteArray key: keys)
            req.addKeys(ByteString.copyFrom(key.get()));
        req.setExact(exact);

        ProtoUtils.writeMessage(output,
                                VProto.VoldemortRequest.newBuilder()
                                                       .setType(RequestType.HAS_KEYS)
                                                       .setStore(storeName)
                                                       .setShouldRoute(routingType.equals(RequestRoutingType.ROUTED))
                                                       .setRequestRouteType(routingType.getRoutingTypeCode())
                                                       .setHasKeys(req)
                                                       .build());
    }

    public Map<ByteArray, Boolean> readHasKeysResponse(DataInputStream input) throws IOException {
        HasKeysResponse.Builder response = ProtoUtils.readToBuilder(input,
                                                                    HasKeysResponse.newBuilder());
        if(response.hasError())
            throwException(response.getError());
        Map<ByteArray, Boolean> vals = new HashMap<ByteArray, Boolean>(response.getValuesCount());
        for(VProto.KeyedBoolean versions: response.getValuesList())
            vals.put(ProtoUtils.decodeBytes(versions.getKey()), versions.getPresent());
        return vals;
    }

    public boolean isCompleteHasKeysResponse(ByteBuffer buffer) {
        return isCompleteResponse(buffer);
    }

}
