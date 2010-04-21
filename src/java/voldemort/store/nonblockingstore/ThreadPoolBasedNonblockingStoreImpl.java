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

import java.util.concurrent.ExecutorService;

import voldemort.VoldemortException;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class ThreadPoolBasedNonblockingStoreImpl implements NonblockingStore {

    private final ExecutorService executor;

    private final Store<ByteArray, byte[]> innerStore;

    public ThreadPoolBasedNonblockingStoreImpl(ExecutorService executor,
                                               Store<ByteArray, byte[]> innerStore) {
        this.executor = Utils.notNull(executor);
        this.innerStore = Utils.notNull(innerStore);
    }

    public void submitGetAllRequest(final Iterable<ByteArray> keys,
                                    final NonblockingStoreCallback callback)
            throws VoldemortException {
        executor.submit(new Runnable() {

            public void run() {
                long start = System.nanoTime();

                try {
                    Object result = innerStore.getAll(keys);

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

    public void submitGetRequest(final ByteArray key, final NonblockingStoreCallback callback)
            throws VoldemortException {
        executor.submit(new Runnable() {

            public void run() {
                long start = System.nanoTime();

                try {
                    Object result = innerStore.get(key);

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

    public void submitGetVersionsRequest(final ByteArray key,
                                         final NonblockingStoreCallback callback) {
        executor.submit(new Runnable() {

            public void run() {
                long start = System.nanoTime();

                try {
                    Object result = innerStore.getVersions(key);

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

    public void submitPutRequest(final ByteArray key,
                                 final Versioned<byte[]> value,
                                 final NonblockingStoreCallback callback) throws VoldemortException {
        executor.submit(new Runnable() {

            public void run() {
                long start = System.nanoTime();

                try {
                    innerStore.put(key, value);

                    if(callback != null)
                        callback.requestComplete(null, (System.nanoTime() - start) / Time.NS_PER_MS);
                } catch(Exception e) {
                    if(callback != null)
                        callback.requestComplete(e, (System.nanoTime() - start) / Time.NS_PER_MS);
                }
            }

        });
    }

    public void submitDeleteRequest(final ByteArray key,
                                    final Version version,
                                    final NonblockingStoreCallback callback)
            throws VoldemortException {
        executor.submit(new Runnable() {

            public void run() {
                long start = System.nanoTime();

                try {
                    Object result = innerStore.delete(key, version);

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

    }

}
