/*
 * Copyright 2008-2010 LinkedIn, Inc
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

    protected final Map<Integer, RebalancePartitionsInfo> stealInfoMap;

    public RebalancerState(List<RebalancePartitionsInfo> stealInfoList) {
        stealInfoMap = Maps.newHashMapWithExpectedSize(stealInfoList.size());
        for(RebalancePartitionsInfo rebalancePartitionsInfo: stealInfoList)
            stealInfoMap.put(rebalancePartitionsInfo.getDonorId(), rebalancePartitionsInfo);
    }

    public static RebalancerState create(String json) {
        List<RebalancePartitionsInfo> stealInfoList = Lists.newLinkedList();
        JsonReader reader = new JsonReader(new StringReader(json));

        for(Object o: reader.readArray()) {
            Map<?, ?> m = (Map<?, ?>) o;
            stealInfoList.add(RebalancePartitionsInfo.create(m));
        }

        return new RebalancerState(stealInfoList);
    }

    public String toJsonString() {
        List<Map<String, Object>> maps = Lists.newLinkedList();

        for(RebalancePartitionsInfo rebalancePartitionsInfo: stealInfoMap.values())
            maps.add(rebalancePartitionsInfo.asMap());

        StringWriter stringWriter = new StringWriter();
        new JsonWriter(stringWriter).write(maps);
        stringWriter.flush();

        return stringWriter.toString();
    }

    public boolean isEmpty() {
        return stealInfoMap.isEmpty();
    }

    public boolean remove(RebalancePartitionsInfo rebalancePartitionsInfo) {
        RebalancePartitionsInfo prev = stealInfoMap.remove(rebalancePartitionsInfo.getDonorId());

        return prev != null;
    }

    public boolean update(RebalancePartitionsInfo rebalancePartitionsInfo) {
        if(!stealInfoMap.containsKey(rebalancePartitionsInfo.getDonorId()))
            return false;

        add(rebalancePartitionsInfo);

        return true;
    }

    public void add(RebalancePartitionsInfo rebalancePartitionsInfo) {
        stealInfoMap.put(rebalancePartitionsInfo.getDonorId(), rebalancePartitionsInfo);
    }

    public Collection<RebalancePartitionsInfo> getAll() {
        return stealInfoMap.values();
    }

    public RebalancePartitionsInfo find(String store, List<Integer> partitionIds) {
        for(int p: partitionIds) {
            for(RebalancePartitionsInfo info: getAll()) {
                if(info.getUnbalancedStoreList().contains(store)
                   && info.getPartitionList().contains(p))
                    return info;
            }
        }

        return null;
    }

    public RebalancePartitionsInfo find(int donorId) {
        return stealInfoMap.get(donorId);
    }

    public List<RebalancePartitionsInfo> find(String store) {
        List<RebalancePartitionsInfo> stealInfoList = Lists.newArrayListWithExpectedSize(stealInfoMap.size());

        for(RebalancePartitionsInfo info: getAll())
            if(info.getUnbalancedStoreList().contains(store))
                stealInfoList.add(info);

        return stealInfoList;
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
        for(RebalancePartitionsInfo info: getAll()) {
            sb.append(info);
            sb.append("\n");
        }
        sb.append(")");

        return sb.toString();
    }
}
