package voldemort.store.grandfather;

/*
 * Copyright 2010 LinkedIn, Inc
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
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.server.StoreRepository;
import voldemort.store.DelegatingStore;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.store.slop.Slop.Operation;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class GrandfatheringStore extends DelegatingStore<ByteArray, byte[], byte[]> {

    private MetadataStore metadata;
    private ExecutorService threadPool;
    private StorageEngine<ByteArray, Slop, byte[]> slopStore;
    private boolean isReadOnly;
    private final Logger logger = Logger.getLogger(getClass());

    public GrandfatheringStore(final Store<ByteArray, byte[], byte[]> innerStore,
                               MetadataStore metadata,
                               StoreRepository storeRepository,
                               ExecutorService threadPool) {
        super(innerStore);
        this.metadata = metadata;
        this.threadPool = threadPool;
        SlopStorageEngine slopEngine = null;
        try {
            slopEngine = storeRepository.getSlopStore();
        } catch(IllegalStateException e) {
            throw new VoldemortException("Grandfathering cannot run without initialization of slop engine",
                                         e);
        }
        this.slopStore = slopEngine.asSlopStore();
        try {
            this.isReadOnly = metadata.getStoreDef(getName())
                                      .getType()
                                      .compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0;
        } catch(Exception e) {
            this.isReadOnly = false;
        }

    }

    @Override
    public void close() throws VoldemortException {
        getInnerStore().close();
    }

    @Override
    public boolean delete(final ByteArray key, final Version version) throws VoldemortException {
        if(isReadOnly)
            throw new UnsupportedOperationException("Delete is not supported on this store, it is read-only.");

        /*
         * Check if this key is one of the grand-fathered keys and accordingly
         * put a delete slop
         */
        if(metadata.getServerState().equals(MetadataStore.VoldemortState.GRANDFATHERING_SERVER)) {
            final List<Integer> mappedPartitions = metadata.getRoutingStrategy(getName())
                                                           .getPartitionList(key.get());
            List<Integer> currentPartitions = metadata.getCluster()
                                                      .getNodeById(metadata.getNodeId())
                                                      .getPartitionIds();
            mappedPartitions.retainAll(currentPartitions);

            if(mappedPartitions.size() != 1) {
                logger.error("Received key which mapped to multiple partitions on single node");
            } else if(metadata.getGrandfatherState().findNodeIds(mappedPartitions.get(0)).size() != 0) {
                this.threadPool.execute(new Runnable() {

                    public void run() {
                        try {
                            Date date = new Date();
                            Set<Integer> futureNodeIds = metadata.getGrandfatherState()
                                                                 .findNodeIds(mappedPartitions.get(0));
                            for(int futureNodeId: futureNodeIds) {
                                Slop slop = new Slop(getName(),
                                                     Operation.DELETE,
                                                     key,
                                                     null,
                                                     null,
                                                     futureNodeId,
                                                     date);
                                try {
                                    slopStore.put(slop.makeKey(),
                                                  Versioned.value(slop, version),
                                                  null);
                                } catch(Exception e) {
                                    if(logger.isDebugEnabled())
                                        logger.debug("Failed to put DELETE operation on "
                                                     + getName() + " to node " + futureNodeId
                                                     + " to slop store", e);
                                }
                            }
                        } catch(Exception e) {
                            logger.warn("Failed to put DELETE operation on " + getName()
                                        + " to slop store", e);
                        }
                    }
                });
            }
        }

        return getInnerStore().delete(key, version);
    }

    @Override
    public void put(final ByteArray key, final Versioned<byte[]> value, final byte[] transform)
            throws VoldemortException {
        if(this.isReadOnly)
            throw new UnsupportedOperationException("Put is not supported on this store, it is read-only.");

        /*
         * Check if this key is one of the grand-fathered keys and accordingly
         * put a put slop
         */
        if(metadata.getServerState().equals(MetadataStore.VoldemortState.GRANDFATHERING_SERVER)) {
            final List<Integer> mappedPartitions = metadata.getRoutingStrategy(getName())
                                                           .getPartitionList(key.get());
            List<Integer> currentPartitions = metadata.getCluster()
                                                      .getNodeById(metadata.getNodeId())
                                                      .getPartitionIds();
            mappedPartitions.retainAll(currentPartitions);

            if(mappedPartitions.size() != 1) {
                logger.error("Received key which mapped to multiple partitions on single node");
            } else if(metadata.getGrandfatherState().findNodeIds(mappedPartitions.get(0)).size() != 0) {
                this.threadPool.execute(new Runnable() {

                    public void run() {
                        try {
                            Date date = new Date();
                            Set<Integer> futureNodeIds = metadata.getGrandfatherState()
                                                                 .findNodeIds(mappedPartitions.get(0));
                            for(int futureNodeId: futureNodeIds) {
                                Slop slop = new Slop(getName(),
                                                     Operation.PUT,
                                                     key,
                                                     value.getValue(),
                                                     transform,
                                                     futureNodeId,
                                                     date);
                                try {
                                    slopStore.put(slop.makeKey(),
                                                  Versioned.value(slop, value.getVersion()),
                                                  null);
                                } catch(Exception e) {
                                    if(logger.isDebugEnabled())
                                        logger.debug("Failed to put PUT operation on " + getName()
                                                     + " to node " + futureNodeId
                                                     + " to slop store", e);
                                }
                            }
                        } catch(Exception e) {
                            logger.warn("Failed to put PUT operation on " + getName()
                                        + " to slop store", e);
                        }
                    }
                });
            }
        }

        getInnerStore().put(key, value, transform);
    }
}
