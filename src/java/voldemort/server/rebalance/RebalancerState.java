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

package voldemort.server.rebalance;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.client.rebalance.RebalanceTaskInfo;
import voldemort.serialization.json.JsonReader;
import voldemort.serialization.json.JsonWriter;
import voldemort.store.metadata.MetadataStore;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Store and manipulate rebalancing state. Moved out from
 * {@link RebalancePartitionsInfo} and {@link MetadataStore}
 * 
 */
public class RebalancerState {

    protected final Map<Integer, RebalanceTaskInfo> stealInfoMap;

    public RebalancerState(List<RebalanceTaskInfo> stealInfoList) {
        stealInfoMap = Maps.newHashMapWithExpectedSize(stealInfoList.size());
        for (RebalanceTaskInfo rebalanceTaskInfo : stealInfoList)
            stealInfoMap.put(rebalanceTaskInfo.getDonorId(), rebalanceTaskInfo);
    }

    public static RebalancerState create(String json) {
        List<RebalanceTaskInfo> stealInfoList = Lists.newLinkedList();
        JsonReader reader = new JsonReader(new StringReader(json));

        for(Object o: reader.readArray()) {
            Map<?, ?> m = (Map<?, ?>) o;
            stealInfoList.add(RebalanceTaskInfo.create(m));
        }

        return new RebalancerState(stealInfoList);
    }

    public String toJsonString() {
        List<Map<String, Object>> maps = Lists.newLinkedList();

        for (RebalanceTaskInfo rebalanceTaskInfo : stealInfoMap.values())
            maps.add(rebalanceTaskInfo.asMap());

        StringWriter stringWriter = new StringWriter();
        new JsonWriter(stringWriter).write(maps);
        stringWriter.flush();

        return stringWriter.toString();
    }

    public boolean isEmpty() {
        return stealInfoMap.isEmpty();
    }

    public boolean remove(RebalanceTaskInfo rebalanceTaskInfo) {
        RebalanceTaskInfo prev = stealInfoMap.remove(rebalanceTaskInfo.getDonorId());
        return prev != null;
    }

    public boolean update(RebalanceTaskInfo rebalanceTaskInfo) {
        if (stealInfoMap.containsKey(rebalanceTaskInfo.getDonorId()))
            return false;

        stealInfoMap.put(rebalanceTaskInfo.getDonorId(), rebalanceTaskInfo);
        return true;
    }

    public Collection<RebalanceTaskInfo> getAll() {
        return stealInfoMap.values();
    }

    // TODO (Sid) : Comment this as part of removing replica type. 
//    public RebalanceTaskInfo find(String storeName,
//            List<Integer> keyPartitions,
//            List<Integer> nodePartitions) {
//        for (RebalanceTaskInfo info : getAll()) {
//
//            // First check if the store exists
//            if (info.getPartitionStores().contains(storeName)) {
//
//                // If yes, check if the key belongs to one of the partitions
//                // being moved
//                if (StoreRoutingPlan.checkKeyBelongsToPartition(keyPartitions,
//                        nodePartitions,
//                        info.getPartitionIds(storeName))) {
//                    return info;
//                }
//            }
//        }
    //
    // // If none of them match, null
    // return null;
    // }

    public RebalanceTaskInfo find(int donorId) {
        return stealInfoMap.get(donorId);
    }

    @Override
    public boolean equals(Object o) {
        if(this == o)
            return true;
        if(o == null || getClass() != o.getClass())
            return false;

        RebalancerState that = (RebalancerState) o;

        if(!stealInfoMap.equals(that.stealInfoMap))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return stealInfoMap.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("RebalancerState(operations: ");
        sb.append("\n");
        for (RebalanceTaskInfo info : getAll()) {
            sb.append(info);
            sb.append("\n");
        }
        sb.append(")");

        return sb.toString();
    }
}