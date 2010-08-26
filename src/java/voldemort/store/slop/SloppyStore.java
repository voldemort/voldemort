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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import voldemort.VoldemortException;
import voldemort.store.DelegatingStore;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.Store;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A Sloppy store is a store wrapper that delegates to an inner store, and if
 * that store fails, instead stores some slop in the first of a list of backup
 * stores. This is a simple consistency mechanism that ensures a transient
 * failure can be recovered from.
 * 
 * This is intended to live on the client side and redeliver messages when a
 * given server fails.
 * 
 * This makes for a somewhat awkward situation when a failure occurs, though:
 * Either we return successfully, in which case the user may believe their data
 * is available when it is not OR we throw an exception, in which case the user
 * may think the update will not occur though eventually it will.
 * 
 * Our choice is to throw a special exception that the user can choose to ignore
 * if they like In the common case, this Store will be used as one of many
 * routed stores, so a single failure will not propagate to the user.
 * 
 * @see voldemort.server.scheduler.SlopPusherJob
 * 
 * 
 */
public class SloppyStore extends DelegatingStore<ByteArray, byte[], byte[]> {

    private final int node;
    // the transforms are useless for backup stores.
    private final List<Store<ByteArray, Slop, byte[]>> backupStores;

    /**
     * Create a store which delegates its operations to it inner store and
     * records write failures in the first available backup store.
     * 
     * @param node The id of the destination node
     * @param innerStore The store which we delegate write operations to
     * @param backupStores A collection of stores which will be used to record
     *        failures, the iterator determines preference.
     */
    public SloppyStore(int node,
                       Store<ByteArray, byte[], byte[]> innerStore,
                       Collection<? extends Store<ByteArray, Slop, byte[]>> backupStores) {
        super(innerStore);
        this.node = node;
        this.backupStores = new ArrayList<Store<ByteArray, Slop, byte[]>>(backupStores);
    }

    /**
     * Attempt to delete from the inner store, if this fails store the operation
     * in the first available SlopStore for eventual consistency.
     */
    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        try {
            return getInnerStore().delete(key, version);
        } catch(UnreachableStoreException e) {
            List<Exception> failures = new ArrayList<Exception>();
            failures.add(e);
            Slop slop = new Slop(getName(),
                                 Slop.Operation.DELETE,
                                 key,
                                 null,
                                 null,
                                 node,
                                 new Date());
            for(Store<ByteArray, Slop, byte[]> slopStore: backupStores) {
                try {
                    slopStore.put(slop.makeKey(), new Versioned<Slop>(slop, version), null);
                    return false;
                } catch(UnreachableStoreException u) {
                    failures.add(u);
                }
            }
            // if we get here that means all backup stores have failed
            throw new InsufficientOperationalNodesException("All slop servers are unavailable from node "
                                                                    + node + ".",
                                                            failures);
        }
    }

    /**
     * Attempt to put to the inner store, if this fails store the operation in
     * the first available SlopStore for eventual consistency.
     */
    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        try {
            getInnerStore().put(key, value, transforms);
        } catch(UnreachableStoreException e) {
            List<Exception> failures = new ArrayList<Exception>();
            failures.add(e);
            boolean persisted = false;
            Slop slop = new Slop(getName(),
                                 Slop.Operation.PUT,
                                 key,
                                 value.getValue(),
                                 transforms,
                                 node,
                                 new Date());
            for(Store<ByteArray, Slop, byte[]> slopStore: backupStores) {
                try {
                    slopStore.put(slop.makeKey(),
                                  new Versioned<Slop>(slop, value.getVersion()),
                                  null);
                    persisted = true;
                    break;
                } catch(UnreachableStoreException u) {
                    failures.add(u);
                }
            }
            if(persisted)
                throw new UnreachableStoreException("Put operation failed on node "
                                                            + node
                                                            + ", but has been persisted to slop storage for eventual replication.",
                                                    e);
            else
                throw new InsufficientOperationalNodesException("All slop servers are unavailable from node "
                                                                        + node + ".",
                                                                failures);
        }
    }

    public List<Store<ByteArray, Slop, byte[]>> getBackupStores() {
        return new ArrayList<Store<ByteArray, Slop, byte[]>>(backupStores);
    }

}
