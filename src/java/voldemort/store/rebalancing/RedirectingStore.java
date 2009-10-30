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

package voldemort.store.rebalancing;

import java.util.List;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.NativeAdminClientRequestFormat;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.socket.SocketPool;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

/**
 * The RedirectingStore extends {@link DelegatingStore}
 * <p>
 * if current server_state is {@link VoldemortState#REBALANCING_MASTER_SERVER} <br>
 * then
 * <ul>
 * <li>Get: proxy Get call to donor server ONLY for keys belonging to
 * {@link MetadataStore#getCurrentPartitionStealList()}</li>
 * <li>Put: do a get() call on donor state and put to innerstore and than
 * handle client put() request to have correct version handling ONLY for keys
 * belonging to {@link MetadataStore#getCurrentPartitionStealList()}.</li>
 * </ul>
 * 
 * @author bbansal
 * 
 */
public class RedirectingStore extends DelegatingStore<ByteArray, byte[]> {

    private final NativeAdminClientRequestFormat adminClient;
    private final MetadataStore metadata;

    public RedirectingStore(Store<ByteArray, byte[]> innerStore,
                            MetadataStore metadata,
                            SocketPool socketPool) {
        super(innerStore);
        this.adminClient = new NativeAdminClientRequestFormat(metadata, socketPool);
        this.metadata = metadata;
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {
        if(MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER.equals(metadata.getServerState())
           && checkKeyBelongsToStolenPartitions(key)) {
            proxyPut(key, value);
        } else {
            getInnerStore().put(key, value);
        }
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        if(MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER.equals(metadata.getServerState())
           && checkKeyBelongsToStolenPartitions(key)) {
            return proxyGet(key);
        } else {
            return getInnerStore().get(key);
        }
    }

    protected boolean checkKeyBelongsToStolenPartitions(ByteArray key) {
        for(int partitionId: metadata.getRoutingStrategy(getName()).getPartitionList(key.get())) {
            if(metadata.getRebalancingPartitionList().contains(partitionId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * performs back-door proxy get to {@link MetadataStore#getDonorNode()}
     * 
     * @param key
     * @return
     * @throws VoldemortException
     */
    protected List<Versioned<byte[]>> proxyGet(ByteArray key) throws VoldemortException {
        if(metadata.getRebalancingProxyDest() != null) {
            return adminClient.redirectGet(metadata.getRebalancingProxyDest().getId(),
                                           getName(),
                                           key);
        }

        throw new VoldemortException("DonorNode not set for proxyGet() ");
    }

    /**
     * In RebalancingStealer state put should be commited on stealer node. <br>
     * to follow voldemort version guarantees stealer <br>
     * node should query donor node and put that value (proxyValue) before
     * committing the value from client.
     * <p>
     * stealer node should ignore {@link ObsoleteVersionException} while
     * commiting proxyValue
     * 
     * 
     * @param key
     * @param value
     * @throws VoldemortException
     */
    protected void proxyPut(ByteArray key, Versioned<byte[]> value) throws VoldemortException {
        List<Versioned<byte[]>> proxyValues = proxyGet(key);

        try {
            for(Versioned<byte[]> proxyValue: proxyValues) {
                getInnerStore().put(key, proxyValue);
            }
        } catch(ObsoleteVersionException e) {
            // ignore these
        }

        // finally put client value
        getInnerStore().put(key, value);
    }
}
