/*
 * Copyright 2013 LinkedIn, Inc
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

import org.apache.log4j.Logger;

import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

/**
 * Task that issues the proxy put against the old replica, based on the old
 * cluster metadata. This is best effort async replication. Failures will be
 * logged and the server log will be post processed in case the rebalancing
 * fails and we move back to old topology
 * 
 * NOTE : There is no need for any special ordering of the proxy puts in the
 * async thread pool (although the threadpool will have a queue of pending proxy
 * puts internally), since a later version being proxy put before an earlier
 * version would simply result in an OVE for the earlier proxy put. Online
 * traffic will not be affected since the proxy node is not a replica and hence
 * no client will be reading from it (if we are at all wondering about read
 * consistency)
 * 
 */
public class AsyncProxyPutTask implements Runnable {

    private final static Logger logger = Logger.getLogger(AsyncProxyPutTask.class);

    private final RedirectingStore redirectingStore;
    private final ByteArray key;
    private final Versioned<byte[]> value;
    private final byte[] transforms;
    private final int destinationNode;
    private final MetadataStore metadata;

    AsyncProxyPutTask(RedirectingStore redirectingStore,
                      ByteArray key,
                      Versioned<byte[]> value,
                      byte[] transforms,
                      int destinationNode) {
        this.key = key;
        this.value = value;
        this.transforms = transforms;
        this.redirectingStore = redirectingStore;
        this.destinationNode = destinationNode;
        this.metadata = redirectingStore.getMetadataStore();
    }

    @Override
    public void run() {
        Node proxyNode = metadata.getCluster().getNodeById(destinationNode);
        long startNs = System.nanoTime();
        try {
            // TODO there are no retries now if the node we want to write to is
            // unavailable
            redirectingStore.checkNodeAvailable(proxyNode);
            Store<ByteArray, byte[], byte[]> socketStore = redirectingStore.getRedirectingSocketStore(redirectingStore.getName(),
                                                                                                      destinationNode);

            socketStore.put(key, value, transforms);
            redirectingStore.recordSuccess(proxyNode, startNs);
            redirectingStore.reportProxyPutSuccess();
            if(logger.isTraceEnabled()) {
                logger.trace("Proxy write for store " + redirectingStore.getName() + " key "
                             + ByteUtils.toHexString(key.get()) + " to destinationNode:"
                             + destinationNode);
            }
        } catch(UnreachableStoreException e) {
            redirectingStore.recordException(proxyNode, startNs, e);
            logFailedProxyPutIfNeeded(e);
        } catch(ObsoleteVersionException ove) {
            /*
             * Proxy puts can get an OVE if somehow there are two stealers for
             * the same proxy node and the other stealer's proxy put already got
             * tothe proxy node.. This will not result from online put winning,
             * since we don't issue proxy puts if the proxy node is still a
             * replica
             */
            logFailedProxyPutIfNeeded(ove);
        } catch(Exception e) {
            // Just log the key.. Not sure having values in the log is a good
            // idea.
            logFailedProxyPutIfNeeded(e);
        }
    }

    private void logFailedProxyPutIfNeeded(Exception e) {
        redirectingStore.reportProxyPutFailure();
        // only log OVE if trace debugging is on.
        if(e instanceof ObsoleteVersionException && !logger.isTraceEnabled()) {
            return;
        }
        logger.error("Exception in proxy put for proxyNode: " + destinationNode + " from node:"
                     + metadata.getNodeId() + " on key " + ByteUtils.toHexString(key.get())
                     + " Version:" + value.getVersion(), e);
    }
}
