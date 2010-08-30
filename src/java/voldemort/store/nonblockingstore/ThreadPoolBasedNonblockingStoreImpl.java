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

package voldemort.store.nonblockingstore;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import voldemort.VoldemortException;
import voldemort.store.Store;
import voldemort.store.StoreRequest;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class ThreadPoolBasedNonblockingStoreImpl implements NonblockingStore {

    private final ExecutorService executor;

    private final Store<ByteArray, byte[], byte[]> innerStore;

    public ThreadPoolBasedNonblockingStoreImpl(ExecutorService executor,
                                               Store<ByteArray, byte[], byte[]> innerStore) {
        this.executor = Utils.notNull(executor);
        this.innerStore = Utils.notNull(innerStore);
    }

    public void submitGetAllRequest(final Iterable<ByteArray> keys,
                                    final Map<ByteArray, byte[]> transforms,
                                    final NonblockingStoreCallback callback) {
        submit(new StoreRequest<Map<ByteArray, List<Versioned<byte[]>>>>() {

            public Map<ByteArray, List<Versioned<byte[]>>> request(Store<ByteArray, byte[], byte[]> store) {
                return innerStore.getAll(keys, transforms);
            }

        },
               callback);
    }

    public void submitGetRequest(final ByteArray key,
                                 final byte[] transforms,
                                 NonblockingStoreCallback callback) {
        submit(new StoreRequest<List<Versioned<byte[]>>>() {

            public List<Versioned<byte[]>> request(Store<ByteArray, byte[], byte[]> store) {
                return innerStore.get(key, transforms);
            }

        }, callback);
    }

    public void submitGetVersionsRequest(final ByteArray key, NonblockingStoreCallback callback) {
        submit(new StoreRequest<List<Version>>() {

            public List<Version> request(Store<ByteArray, byte[], byte[]> store) {
                return innerStore.getVersions(key);
            }

        }, callback);
    }

    public void submitPutRequest(final ByteArray key,
                                 final Versioned<byte[]> value,
                                 final byte[] transforms,
                                 NonblockingStoreCallback callback) {
        submit(new StoreRequest<Void>() {

            public Void request(Store<ByteArray, byte[], byte[]> store) {
                innerStore.put(key, value, transforms);
                return null;
            }

        }, callback);
    }

    public void submitDeleteRequest(final ByteArray key,
                                    final Version version,
                                    NonblockingStoreCallback callback) {
        submit(new StoreRequest<Boolean>() {

            public Boolean request(Store<ByteArray, byte[], byte[]> store) {
                return innerStore.delete(key, version);
            }

        }, callback);
    }

    private void submit(final StoreRequest<?> request, final NonblockingStoreCallback callback) {
        executor.submit(new Runnable() {

            public void run() {
                long start = System.nanoTime();

                try {
                    Object result = request.request(innerStore);

                    if(callback != null)
                        callback.requestComplete(result, (System.nanoTime() - start)
                                                         / Time.NS_PER_MS);
                } catch(Exception e) {
                    if(callback != null)
                        callback.requestComplete(e, (System.nanoTime() - start) / Time.NS_PER_MS);
                }
            }

        });
    }

    public void close() throws VoldemortException {
        innerStore.close();
    }

}
