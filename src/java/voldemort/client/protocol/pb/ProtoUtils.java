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
import voldemort.client.protocol.pb.VAdminProto.PartitionTuple;
import voldemort.client.protocol.pb.VAdminProto.PerStorePartitionTuple;
import voldemort.client.protocol.pb.VAdminProto.ROStoreVersionDirMap;
import voldemort.client.protocol.pb.VAdminProto.RebalancePartitionInfoMap;
import voldemort.client.rebalance.RebalancePartitionsInfo;
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

    /**
     * Given a list protobuf rebalance-partition info, converts it into a list
     * of our rebalance-partition info
     * 
     * @param rebalancePartitionInfoMaps List of protobuff version of
     *        rebalance-partition-info
     * @return List of our Rebalance-partition-info
     */
    public static List<RebalancePartitionsInfo> decodeRebalancePartitionInfoMap(List<VAdminProto.RebalancePartitionInfoMap> rebalancePartitionInfoMaps) {
        List<RebalancePartitionsInfo> infos = Lists.newArrayList();
        for(RebalancePartitionInfoMap map: rebalancePartitionInfoMaps) {
            infos.add(decodeRebalancePartitionInfoMap(map));
        }
        return infos;
    }

    /**
     * Given a list of rebalance-partition info, convert it into a list of
     * protobuf equivalent
     * 
     * @param stealInfos List of rebalance partition info
     * @return Protobuff equivalent of the same
     */
    public static List<RebalancePartitionInfoMap> encodeRebalancePartitionInfoMap(List<RebalancePartitionsInfo> stealInfos) {
        List<RebalancePartitionInfoMap> maps = Lists.newArrayList();
        for(RebalancePartitionsInfo info: stealInfos) {
            maps.add(encodeRebalancePartitionInfoMap(info));
        }
        return maps;
    }

    /**
     * Given a protobuf rebalance-partition info, converts it into our
     * rebalance-partition info
     * 
     * @param rebalancePartitionInfoMap Proto-buff version of
     *        rebalance-partition-info
     * @return Rebalance-partition-info
     */
    public static RebalancePartitionsInfo decodeRebalancePartitionInfoMap(VAdminProto.RebalancePartitionInfoMap rebalancePartitionInfoMap) {
        RebalancePartitionsInfo rebalanceStealInfo = new RebalancePartitionsInfo(rebalancePartitionInfoMap.getStealerId(),
                                                                                 rebalancePartitionInfoMap.getDonorId(),
                                                                                 decodePerStorePartitionTuple(rebalancePartitionInfoMap.getReplicaToAddPartitionList()),
                                                                                 decodePerStorePartitionTuple(rebalancePartitionInfoMap.getReplicaToDeletePartitionList()),
                                                                                 new ClusterMapper().readCluster(new StringReader(rebalancePartitionInfoMap.getInitialCluster())),
                                                                                 rebalancePartitionInfoMap.getAttempt());
        return rebalanceStealInfo;
    }

    /**
     * Given a rebalance-partition info, convert it into the protobuf equivalent
     * 
     * @param stealInfo Rebalance partition info
     * @return Protobuff equivalent of the same
     */
    public static RebalancePartitionInfoMap encodeRebalancePartitionInfoMap(RebalancePartitionsInfo stealInfo) {
        return RebalancePartitionInfoMap.newBuilder()
                                        .setStealerId(stealInfo.getStealerId())
                                        .setDonorId(stealInfo.getDonorId())
                                        .addAllReplicaToAddPartition(ProtoUtils.encodePerStorePartitionTuple(stealInfo.getStoreToReplicaToAddPartitionList()))
                                        .addAllReplicaToDeletePartition(ProtoUtils.encodePerStorePartitionTuple(stealInfo.getStoreToReplicaToDeletePartitionList()))
                                        .setInitialCluster(new ClusterMapper().writeCluster(stealInfo.getInitialCluster()))
                                        .setAttempt(stealInfo.getAttempt())
                                        .build();
    }

    public static Map<String, String> encodeROMap(List<ROStoreVersionDirMap> metadataMap) {
        Map<String, String> storeToValue = Maps.newHashMap();
        for(ROStoreVersionDirMap currentStore: metadataMap) {
            storeToValue.put(currentStore.getStoreName(), currentStore.getStoreDir());
        }
        return storeToValue;
    }

    /**
     * Given a map of replica type to partitions, converts it to proto-buff
     * equivalent called PartitionTuple
     * 
     * @param replicaToPartitionList Map of replica type to partitions
     * @return List of partition tuples
     */
    public static List<PartitionTuple> encodePartitionTuple(HashMap<Integer, List<Integer>> replicaToPartitionList) {
        List<PartitionTuple> tuples = Lists.newArrayList();
        for(Entry<Integer, List<Integer>> entry: replicaToPartitionList.entrySet()) {
            PartitionTuple.Builder tupleBuilder = PartitionTuple.newBuilder();
            tupleBuilder.setReplicaType(entry.getKey());
            tupleBuilder.addAllPartitions(entry.getValue());
            tuples.add(tupleBuilder.build());
        }
        return tuples;
    }

    /**
     * 
     * @param partitionTuples
     * @return HashMap of replica type (Integer) to list of partition IDs
     *         (Integer)
     */
    public static HashMap<Integer, List<Integer>> decodePartitionTuple(List<PartitionTuple> partitionTuples) {
        HashMap<Integer, List<Integer>> replicaToPartitionList = Maps.newHashMap();
        for(PartitionTuple tuple: partitionTuples) {
            replicaToPartitionList.put(tuple.getReplicaType(), tuple.getPartitionsList());
        }
        return replicaToPartitionList;
    }

    public static List<PerStorePartitionTuple> encodePerStorePartitionTuple(HashMap<String, HashMap<Integer, List<Integer>>> storeToReplicaToPartitionList) {
        List<PerStorePartitionTuple> perStorePartitionTuples = Lists.newArrayList();
        for(Entry<String, HashMap<Integer, List<Integer>>> entry: storeToReplicaToPartitionList.entrySet()) {
            PerStorePartitionTuple.Builder tupleBuilder = PerStorePartitionTuple.newBuilder();
            tupleBuilder.setStoreName(entry.getKey());
            tupleBuilder.addAllReplicaToPartition(encodePartitionTuple(entry.getValue()));
            perStorePartitionTuples.add(tupleBuilder.build());
        }
        return perStorePartitionTuples;
    }

    public static HashMap<String, HashMap<Integer, List<Integer>>> decodePerStorePartitionTuple(List<PerStorePartitionTuple> perStorePartitionTuples) {
        HashMap<String, HashMap<Integer, List<Integer>>> storeToReplicaToPartitionList = Maps.newHashMap();
        for(PerStorePartitionTuple tuple: perStorePartitionTuples) {
            storeToReplicaToPartitionList.put(tuple.getStoreName(),
                                              decodePartitionTuple(tuple.getReplicaToPartitionList()));
        }
        return storeToReplicaToPartitionList;
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
