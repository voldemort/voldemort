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

package voldemort.store.slop;

import java.util.Date;
import java.util.List;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A delegating store that calculates ownership and stores unowned objects in a
 * special slop store for later delivery to their rightful owner.
 * 
 * This store is intended to live on the server side, and check that data the
 * server receives was properly routed.
 * 
 * @see voldemort.server.scheduler.SlopPusherJob
 * 
 * @author jay
 * 
 */
public class SlopDetectingStore extends DelegatingStore<byte[], byte[]> {

    private final int replicationFactor;
    private final Node localNode;
    private final RoutingStrategy routingStrategy;
    private final Store<byte[], Slop> slopStore;

    public SlopDetectingStore(Store<byte[], byte[]> innerStore,
                              Store<byte[], Slop> slopStore,
                              int replicationFactor,
                              Node localNode,
                              RoutingStrategy routingStrategy) {
        super(innerStore);
        this.replicationFactor = replicationFactor;
        this.localNode = Utils.notNull(localNode);
        this.routingStrategy = Utils.notNull(routingStrategy);
        this.slopStore = Utils.notNull(slopStore);
    }

    private boolean isLocal(byte[] key) {
        List<Node> nodes = routingStrategy.routeRequest(key);
        int index = nodes.indexOf(localNode);
        return index >= 0 && index < replicationFactor;
    }

    @Override
    public boolean delete(byte[] key, Version version) throws VoldemortException {
        if(isLocal(key)) {
            return getInnerStore().delete(key, version);
        } else {
            Slop slop = new Slop(getName(),
                                 Slop.Operation.DELETE,
                                 key,
                                 null,
                                 localNode.getId(),
                                 new Date());
            slopStore.put(slop.makeKey(), new Versioned<Slop>(slop, version));
            return false;
        }
    }

    @Override
    public void put(byte[] key, Versioned<byte[]> value) throws VoldemortException {
        if(isLocal(key)) {
            getInnerStore().put(key, value);
        } else {
            Slop slop = new Slop(getName(),
                                 Slop.Operation.PUT,
                                 key,
                                 value.getValue(),
                                 localNode.getId(),
                                 new Date());
            slopStore.put(slop.makeKey(), new Versioned<Slop>(slop, value.getVersion()));
        }
    }

}
