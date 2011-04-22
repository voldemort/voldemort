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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.serialization.json.JsonReader;
import voldemort.serialization.json.JsonWriter;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.RebalancePartitionsInfoLifeCycleStatus;
import voldemort.utils.Utils;

import com.google.common.collect.Lists;

/**
 * Store and manipulate rebalancing state. Moved out from
 * {@link RebalancePartitionsInfo} and {@link MetadataStore}
 * 
 */
public class RebalancerState {

    private final ConcurrentMap<Integer, RebalancePartitionsInfoLiveCycle> stealInfoMap;

    /**
     * Initializes internal map structure that keeps track of donorIDs to
     * {@link RebalancePartitionsInfoLiveCycle}.
     * <p>
     * 
     * Each association represents a rebalance action between the stealer and
     * the donor in question.
     * 
     * The execution state of this rebalance action is track using
     * {@link RebalancePartitionsInfoLiveCycle} which contains an instance of
     * {@link RebalancePartitionsInfoLifeCycleStatus}. All the rebalance task
     * will be initialized as {@link RebalancePartitionsInfoLifeCycleStatus#NEW}
     * from the execution point of view.
     * <p>
     * 
     * Note: If a stealerNode dies while rebalancing is taking place we have to
     * resume this failed task after the stealerNode is brought back to
     * operation.
     * 
     * 
     * @param stealInfoList list of {@link RebalancePartitionsInfo} used to
     *        initialized the internal map structure <code>stealInfoMap</code>
     */
    public RebalancerState(List<RebalancePartitionsInfo> stealInfoList) {
        // Prevent ConcurrentModificationException.
        stealInfoMap = new ConcurrentHashMap<Integer, RebalancePartitionsInfoLiveCycle>(stealInfoList.size());

        for(RebalancePartitionsInfo rebalancePartitionsInfo: stealInfoList) {
            RebalancePartitionsInfoLiveCycle pinfoLiveCycle = new RebalancePartitionsInfoLiveCycle(rebalancePartitionsInfo,
                                                                                                   RebalancePartitionsInfoLifeCycleStatus.NEW);
            stealInfoMap.put(rebalancePartitionsInfo.getDonorId(), pinfoLiveCycle);
        }

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

        for(RebalancePartitionsInfoLiveCycle rebalancePartitionsInfoLiveCycle: stealInfoMap.values()) {
            maps.add(rebalancePartitionsInfoLiveCycle.getRebalancePartitionsInfo().asMap());
        }

        StringWriter stringWriter = new StringWriter();
        new JsonWriter(stringWriter).write(maps);
        stringWriter.flush();

        return stringWriter.toString();
    }

    public boolean isEmpty() {
        return stealInfoMap.isEmpty();
    }

    public boolean remove(RebalancePartitionsInfo rebalancePartitionsInfo) {
        RebalancePartitionsInfoLiveCycle prev = stealInfoMap.remove(rebalancePartitionsInfo.getDonorId());
        return prev != null;
    }

    public boolean update(final RebalancePartitionsInfoLiveCycle rebalancePartitionsInfoLiveCycle) {
        RebalancePartitionsInfo rebalancePartitionsInfo = rebalancePartitionsInfoLiveCycle.getRebalancePartitionsInfo();
        if(!stealInfoMap.containsKey(rebalancePartitionsInfo.getDonorId()))
            return false;

        add(rebalancePartitionsInfoLiveCycle);

        return true;
    }

    public void add(final RebalancePartitionsInfoLiveCycle rebalancePartitionsInfoLiveCycle) {
        int donorId = rebalancePartitionsInfoLiveCycle.getRebalancePartitionsInfo().getDonorId();
        stealInfoMap.put(donorId, rebalancePartitionsInfoLiveCycle);
    }

    public void add(RebalancePartitionsInfo rebalancePartitionsInfo,
                    RebalancePartitionsInfoLifeCycleStatus status) {
        stealInfoMap.put(rebalancePartitionsInfo.getDonorId(),
                         new RebalancePartitionsInfoLiveCycle(rebalancePartitionsInfo, status));
    }

    public Collection<RebalancePartitionsInfoLiveCycle> getAll() {
        return stealInfoMap.values();
    }

    public RebalancePartitionsInfoLiveCycle find(String store, List<Integer> partitionIds) {
        for(int p: partitionIds) {
            for(RebalancePartitionsInfoLiveCycle info: getAll()) {
                if(info.getRebalancePartitionsInfo().getUnbalancedStoreList().contains(store)
                   && info.getRebalancePartitionsInfo().getPartitionList().contains(p))
                    return info;
            }
        }

        return null;
    }

    public RebalancePartitionsInfoLiveCycle find(int donorId) {
        return stealInfoMap.get(donorId);
    }

    public List<RebalancePartitionsInfoLiveCycle> find(String store) {
        List<RebalancePartitionsInfoLiveCycle> stealInfoList = Lists.newArrayListWithExpectedSize(stealInfoMap.size());

        for(RebalancePartitionsInfoLiveCycle info: getAll())
            if(info.getRebalancePartitionsInfo().getUnbalancedStoreList().contains(store))
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
        sb.append(Utils.NEWLINE);
        for(RebalancePartitionsInfoLiveCycle info: getAll()) {
            sb.append(info);
            sb.append(Utils.NEWLINE);
        }
        sb.append(")");

        return sb.toString();
    }
}
