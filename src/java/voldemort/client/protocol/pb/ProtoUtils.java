/*
 * Copyright 2008-2013 LinkedIn, Inc
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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import voldemort.VoldemortException;
import voldemort.client.protocol.pb.VAdminProto.ROStoreVersionDirMap;
import voldemort.client.protocol.pb.VAdminProto.RebalanceTaskInfoMap;
import voldemort.client.protocol.pb.VAdminProto.StoreToPartitionsIds;
import voldemort.client.protocol.pb.VProto.KeyedVersions;
import voldemort.client.rebalance.RebalanceTaskInfo;
import voldemort.store.ErrorCodeMapper;
import voldemort.utils.ByteArray;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;

/**
 * Helper functions for serializing or deserializing client requests in protocol
 * buffers
 * 
 * 
 */
public class ProtoUtils {

    /*
     * Begin decode RebalanceTaskInfoMap methods
     */

    /**
     * Given a protobuf rebalance-partition info, converts it into our
     * rebalance-partition info
     * 
     * @param rebalanceTaskInfoMap Proto-buff version of
     *        RebalanceTaskInfoMap
     * @return RebalanceTaskInfo object.
     */
    public static RebalanceTaskInfo decodeRebalanceTaskInfoMap(VAdminProto.RebalanceTaskInfoMap rebalanceTaskInfoMap) {
        RebalanceTaskInfo rebalanceTaskInfo = new RebalanceTaskInfo(
                                                                    rebalanceTaskInfoMap.getStealerId(),
                                                                    rebalanceTaskInfoMap.getDonorId(),
                                                                    decodeStoreToPartitionIds(rebalanceTaskInfoMap.getPerStorePartitionIdsList()),
                                                                    new ClusterMapper().readCluster(new StringReader(rebalanceTaskInfoMap.getInitialCluster())));
        return rebalanceTaskInfo;
    }

    public static HashMap<String, List<Integer>> decodeStoreToPartitionIds(List<StoreToPartitionsIds> storeToPartitionIds) {
        HashMap<String, List<Integer>> storeToPartitionIdsList = Maps.newHashMap();
        for(StoreToPartitionsIds tuple: storeToPartitionIds) {
            storeToPartitionIdsList.put(tuple.getStoreName(), tuple.getPartitionIdsList());
        }
        return storeToPartitionIdsList;
    }

    /*
     * End decode RebalanceTaskInfoMap methods
     */

    /*
     * Begin encode RebalanceTaskInfo methods
     */

    /**
     * Given a rebalance-task info, convert it into the protobuf equivalent
     * 
     * @param stealInfo Rebalance task info
     * @return Protobuf equivalent of the same
     */
    public static RebalanceTaskInfoMap encodeRebalanceTaskInfoMap(RebalanceTaskInfo stealInfo) {
        return RebalanceTaskInfoMap.newBuilder()
                                   .setStealerId(stealInfo.getStealerId())
                                   .setDonorId(stealInfo.getDonorId())
                                   .addAllPerStorePartitionIds(ProtoUtils.encodeStoreToPartitionsTuple(stealInfo.getStoreToPartitionIds()))
                                   .setInitialCluster(new ClusterMapper().writeCluster(stealInfo.getInitialCluster()))
                                   .build();
    }

    public static List<StoreToPartitionsIds> encodeStoreToPartitionsTuple(HashMap<String, List<Integer>> storeToPartitionIds) {
        List<StoreToPartitionsIds> perStorePartitionTuples = Lists.newArrayList();
        for(Entry<String, List<Integer>> entry: storeToPartitionIds.entrySet()) {
            StoreToPartitionsIds.Builder tupleBuilder = StoreToPartitionsIds.newBuilder();
            tupleBuilder.setStoreName(entry.getKey());
            tupleBuilder.addAllPartitionIds(entry.getValue());
            perStorePartitionTuples.add(tupleBuilder.build());
        }
        return perStorePartitionTuples;
    }

    /*
     * End encode RebalanceTaskInfo methods
     */

    public static Map<String, String> encodeROMap(List<ROStoreVersionDirMap> metadataMap) {
        Map<String, String> storeToValue = Maps.newHashMap();
        for(ROStoreVersionDirMap currentStore: metadataMap) {
            storeToValue.put(currentStore.getStoreName(), currentStore.getStoreDir());
        }
        return storeToValue;
    }

    public static VProto.Error.Builder encodeError(ErrorCodeMapper mapper, VoldemortException e) {
        return VProto.Error.newBuilder()
                           .setErrorCode(mapper.getCode(e))
                           .setErrorMessage(e.getMessage());
    }

    public static VProto.Versioned.Builder encodeVersioned(Versioned<byte[]> versioned) {
        return VProto.Versioned.newBuilder()
                               .setValue(ByteString.copyFrom(versioned.getValue()))
                               .setVersion(ProtoUtils.encodeClock(versioned.getVersion()));
    }

    public static Versioned<byte[]> decodeVersioned(VProto.Versioned versioned) {
        return new Versioned<byte[]>(versioned.getValue().toByteArray(),
                                     decodeClock(versioned.getVersion()));
    }

    /**
     * Given a list of value versions for the metadata keys we are just
     * interested in the value at index 0 This is because even if we have to
     * update the cluster.xml we marshall a single key into a versioned list
     * Hence we just look at the value at index 0
     * 
     */
    public static Versioned<byte[]> decodeVersionedMetadataKeyValue(KeyedVersions keyValue) {
        return decodeVersioned(keyValue.getVersions(0));
    }

    public static List<Versioned<byte[]>> decodeVersions(List<VProto.Versioned> versioned) {
        List<Versioned<byte[]>> values = new ArrayList<Versioned<byte[]>>(versioned.size());
        for(VProto.Versioned v: versioned)
            values.add(decodeVersioned(v));
        return values;
    }

    public static VectorClock decodeClock(VProto.VectorClock encoded) {
        List<ClockEntry> entries = new ArrayList<ClockEntry>(encoded.getEntriesCount());
        for(VProto.ClockEntry entry: encoded.getEntriesList())
            entries.add(new ClockEntry((short) entry.getNodeId(), entry.getVersion()));
        return new VectorClock(entries, encoded.getTimestamp());
    }

    public static VProto.VectorClock.Builder encodeClock(Version version) {
        VectorClock clock = (VectorClock) version;
        VProto.VectorClock.Builder encoded = VProto.VectorClock.newBuilder();
        encoded.setTimestamp(clock.getTimestamp());
        for(ClockEntry entry: clock.getEntries())
            encoded.addEntries(VProto.ClockEntry.newBuilder()
                                                .setNodeId(entry.getNodeId())
                                                .setVersion(entry.getVersion()));
        return encoded;
    }

    public static ByteArray decodeBytes(ByteString string) {
        return new ByteArray(string.toByteArray());
    }

    public static ByteString encodeBytes(ByteArray array) {
        return ByteString.copyFrom(array.get());
    }

    public static ByteString encodeTransform(byte[] transform) {
        return ByteString.copyFrom(transform);
    }

    public static void writeMessage(DataOutputStream output, Message message) throws IOException {
        /*
         * We don't use varints here because the c++ version of the protocol
         * buffer classes seem to be buggy requesting more data than necessary
         * from the underlying stream causing it to block forever
         */
        output.writeInt(message.getSerializedSize());
        CodedOutputStream codedOut = CodedOutputStream.newInstance(output);
        message.writeTo(codedOut);
        codedOut.flush();
    }

    public static void writeEndOfStream(DataOutputStream output) throws IOException {
        output.writeInt(-1);
    }

    public static <T extends Message.Builder> T readToBuilder(DataInputStream input, T builder)
            throws IOException {
        int size = input.readInt();
        CodedInputStream codedIn = CodedInputStream.newInstance(input);
        codedIn.pushLimit(size);
        builder.mergeFrom(codedIn);
        return builder;
    }
}
