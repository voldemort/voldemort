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

package voldemort.server.scheduler;

import java.util.List;

import org.apache.log4j.Logger;

import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.server.StoreRepository;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Time;
import voldemort.versioning.Versioned;

/**
 * A job that checks each key to see if it belongs on the current node. If it
 * does not, it does a PUT on the key to assure that it is written to its proper
 * home and then deletes it from the local node.
 * 
 * 
 */
public class RebalancingJob implements Runnable {

    private static Logger logger = Logger.getLogger(RebalancingJob.class);

    private final int localNodeId;
    private final RoutingStrategy router;
    private final StoreRepository storeRepository;

    public RebalancingJob(int localNodeId, RoutingStrategy router, StoreRepository storeRepository) {
        this.localNodeId = localNodeId;
        this.storeRepository = storeRepository;
        this.router = router;
    }

    public void run() {
        logger.info("Rebalancing all keys...");
        int totalRebalanced = 0;
        long start = System.currentTimeMillis();
        for(StorageEngine<ByteArray, byte[], byte[]> engine: storeRepository.getAllStorageEngines()) {
            logger.info("Rebalancing " + engine.getName());
            Store<ByteArray, byte[], byte[]> remote = storeRepository.getRoutedStore(engine.getName());
            ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iterator = engine.entries();
            int rebalanced = 0;
            long currStart = System.currentTimeMillis();
            while(iterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> keyAndVal = iterator.next();
                if(needsRebalancing(keyAndVal.getFirst())) {
                    remote.put(keyAndVal.getFirst(), keyAndVal.getSecond(), null);
                    engine.delete(keyAndVal.getFirst(), keyAndVal.getSecond().getVersion());
                    rebalanced++;
                }
            }
            totalRebalanced += rebalanced;
            long ellapsedSeconds = (System.currentTimeMillis() - currStart) / Time.MS_PER_SECOND;
            logger.info("Rebalancing of store " + engine.getName() + " completed in "
                        + ellapsedSeconds + " seconds.");
            logger.info(rebalanced + " keys rebalanced.");
        }
        long ellapsedSeconds = (System.currentTimeMillis() - start) / Time.MS_PER_SECOND;
        logger.info("Rebalancing complete for all stores in " + ellapsedSeconds + " seconds.");
        logger.info(totalRebalanced + " keys rebalanced in total.");
    }

    private boolean needsRebalancing(ByteArray key) {
        List<Node> responsible = router.routeRequest(key.get());
        for(Node n: responsible)
            if(n.getId() == localNodeId)
                return false;
        return true;
    }

}
