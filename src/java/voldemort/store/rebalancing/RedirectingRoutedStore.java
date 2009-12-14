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
import java.util.Map;
import java.util.concurrent.ExecutorService;

import voldemort.client.DefaultStoreClient;
import voldemort.cluster.Cluster;
import voldemort.server.StoreRepository;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.routed.RoutedStore;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The RedirectingRoutedStore extends {@link RoutedStore}
 * 
 * catch all InvalidMetadataException and updates the routed store with latest
 * cluster metadata.<br>
 * ServerSide Routing side of re-bootstrap implemented in
 * {@link DefaultStoreClient}
 * 
 */
public class RedirectingRoutedStore extends RoutedStore {

    private final MetadataStore metadata;
    private final StoreRepository storeRepository;
    private int maxMetadataRefreshAttempts = 3;

    public RedirectingRoutedStore(MetadataStore metadataStore,
                                  StoreRepository storeRepository,
                                  String name,
                                  Map<Integer, Store<ByteArray, byte[]>> innerStores,
                                  Cluster cluster,
                                  StoreDefinition storeDef,
                                  boolean repairReads,
                                  ExecutorService threadPool,
                                  long timeoutMs,
                                  long nodeBannageMs,
                                  Time time) {
        super(name,
              innerStores,
              cluster,
              storeDef,
              repairReads,
              threadPool,
              timeoutMs,
              nodeBannageMs,
              time);

        this.metadata = metadataStore;
        this.storeRepository = storeRepository;
    }

    private void reinit() {
        super.updateRoutingStrategy(metadata.getRoutingStrategy(getName()));
    }

    @Override
    public boolean delete(final ByteArray key, final Version version) {
        for(int attempts = 0; attempts < this.maxMetadataRefreshAttempts; attempts++) {
            try {
                return super.delete(key, version);
            } catch(InvalidMetadataException e) {
                reinit();
            }
        }
        throw new InvalidMetadataException(this.maxMetadataRefreshAttempts
                                           + " metadata refresh attempts failed.");
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        for(int attempts = 0; attempts < this.maxMetadataRefreshAttempts; attempts++) {
            try {
                return super.getVersions(key);
            } catch(InvalidMetadataException e) {
                reinit();
            }
        }
        throw new InvalidMetadataException(this.maxMetadataRefreshAttempts
                                           + " metadata refresh attempts failed.");
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key) {
        for(int attempts = 0; attempts < this.maxMetadataRefreshAttempts; attempts++) {
            try {
                return super.get(key);
            } catch(InvalidMetadataException e) {
                reinit();
            }
        }
        throw new InvalidMetadataException(this.maxMetadataRefreshAttempts
                                           + " metadata refresh attempts failed.");
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys) {
        for(int attempts = 0; attempts < this.maxMetadataRefreshAttempts; attempts++) {
            try {
                return super.getAll(keys);
            } catch(InvalidMetadataException e) {
                reinit();
            }
        }
        throw new InvalidMetadataException(this.maxMetadataRefreshAttempts
                                           + " metadata refresh attempts failed.");
    }

    @Override
    public void put(final ByteArray key, final Versioned<byte[]> versioned)
            throws ObsoleteVersionException {
        for(int attempts = 0; attempts < this.maxMetadataRefreshAttempts; attempts++) {
            try {
                super.put(key, versioned);
                return;
            } catch(InvalidMetadataException e) {
                reinit();
            }
        }
        throw new InvalidMetadataException(this.maxMetadataRefreshAttempts
                                           + " metadata refresh attempts failed.");
    }
}
