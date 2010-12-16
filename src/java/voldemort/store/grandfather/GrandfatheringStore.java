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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import voldemort.VoldemortException;
import voldemort.server.StoreRepository;
import voldemort.store.DelegatingStore;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class GrandfatheringStore extends DelegatingStore<ByteArray, byte[], byte[]> {

    private MetadataStore metadata;
    private ExecutorService service;
    private StorageEngine<ByteArray, Slop, byte[]> slopStore;
    private boolean isReadOnly;

    public GrandfatheringStore(final Store<ByteArray, byte[], byte[]> innerStore,
                               MetadataStore metadata,
                               StoreRepository storeRepository) {
        super(innerStore);
        this.metadata = metadata;
        this.service = Executors.newSingleThreadExecutor(new ThreadFactory() {

            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("grandfather-thread-" + innerStore.getName());
                return thread;
            }
        });
        SlopStorageEngine slopEngine = null;
        try {
            slopEngine = storeRepository.getSlopStore();
        } catch(IllegalStateException e) {
            throw new VoldemortException("Grandfathering cannot run without initialization of slop engine");
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
        this.service.shutdown();
        getInnerStore().close();
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        if(isReadOnly)
            throw new UnsupportedOperationException("Delete is not supported on this store, it is read-only.");

        /*
         * Check if this key is one of the grandfathered keys and accordingly
         * put a delete slop
         */
        return getInnerStore().delete(key, version);
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transform)
            throws VoldemortException {
        if(this.isReadOnly)
            throw new UnsupportedOperationException("Put is not supported on this store, it is read-only.");

        /*
         * Check if this key is one of the grandfatehered keys and accordingly
         * put a put slop
         */
        getInnerStore().put(key, value, transform);
    }

}
