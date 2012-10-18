/*
 * Copyright 2012 LinkedIn, Inc
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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.Store;
import voldemort.store.StoreRequest;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class ThreadPoolBasedNonblockingStoreImpl implements NonblockingStore {

    private final ExecutorService executor;

    private final Store<ByteArray, byte[], byte[]> innerStore;

    private final Logger logger = Logger.getLogger(ThreadPoolBasedNonblockingStoreImpl.class);

    public ThreadPoolBasedNonblockingStoreImpl(ExecutorService executor,
                                               Store<ByteArray, byte[], byte[]> innerStore) {
        this.executor = Utils.notNull(executor);
        this.innerStore = Utils.notNull(innerStore);
    }

    public void submitGetAllRequest(final Iterable<ByteArray> keys,
                                    final Map<ByteArray, byte[]> transforms,
                                    final NonblockingStoreCallback callback,
                                    long timeoutMs) {
        submit(new StoreRequest<Map<ByteArray, List<Versioned<byte[]>>>>() {

            public Map<ByteArray, List<Versioned<byte[]>>> request(Store<ByteArray, byte[], byte[]> store) {
                return innerStore.getAll(keys, transforms);
            }
        },
               callback,
               timeoutMs,
               "get all");
    }

    public void submitGetRequest(final ByteArray key,
                                 final byte[] transforms,
                                 NonblockingStoreCallback callback,
                                 long timeoutMs) {
        submit(new StoreRequest<List<Versioned<byte[]>>>() {

            public List<Versioned<byte[]>> request(Store<ByteArray, byte[], byte[]> store) {
                return innerStore.get(key, transforms);
            }

        }, callback, timeoutMs, "get");
    }

    public void submitGetVersionsRequest(final ByteArray key,
                                         NonblockingStoreCallback callback,
                                         long timeoutMs) {
        submit(new StoreRequest<List<Version>>() {

            public List<Version> request(Store<ByteArray, byte[], byte[]> store) {
                return innerStore.getVersions(key);
            }

        }, callback, timeoutMs, "submit");
    }

    public void submitPutRequest(final ByteArray key,
                                 final Versioned<byte[]> value,
                                 final byte[] transforms,
                                 NonblockingStoreCallback callback,
                                 long timeoutMs) {
        submit(new StoreRequest<Void>() {

            public Void request(Store<ByteArray, byte[], byte[]> store) {
                innerStore.put(key, value, transforms);
                return null;
            }

        }, callback, timeoutMs, "put");
    }

    public void submitDeleteRequest(final ByteArray key,
                                    final Version version,
                                    NonblockingStoreCallback callback,
                                    long timeoutMs) {
        submit(new StoreRequest<Boolean>() {

            public Boolean request(Store<ByteArray, byte[], byte[]> store) {
                return innerStore.delete(key, version);
            }

        }, callback, timeoutMs, "delete");
    }

    private void submit(final StoreRequest<?> request,
                        final NonblockingStoreCallback callback,
                        final long timeoutMs,
                        final String operationName) {
        executor.submit(new Runnable() {

            public void run() {
                long start = System.nanoTime();
                final long timeoutNs = timeoutMs * Time.NS_PER_MS;

                try {
                    Object result = request.request(innerStore);

                    if(callback != null) {
                        long diff = System.nanoTime() - start;

                        if(diff <= timeoutNs) {
                            try {
                                callback.requestComplete(result, diff / Time.NS_PER_MS);
                            } catch(Exception e) {
                                if(logger.isEnabledFor(Level.WARN))
                                    logger.warn(e, e);
                            }
                        } else {
                            UnreachableStoreException ex = new UnreachableStoreException("Failure in "
                                                                                         + operationName
                                                                                         + ": time out exceeded");
                            try {
                                callback.requestComplete(ex, diff / Time.NS_PER_MS);
                            } catch(Exception e) {
                                if(logger.isEnabledFor(Level.WARN))
                                    logger.warn(e, e);
                            }
                        }
                    }
                } catch(Exception e) {
                    if(callback != null) {
                        long diff = System.nanoTime() - start;

                        try {
                            callback.requestComplete(e, diff / Time.NS_PER_MS);
                        } catch(Exception ex) {
                            if(logger.isEnabledFor(Level.WARN))
                                logger.warn(ex, ex);
                        }
                    }
                }
            }

        });
    }

    public void close() throws VoldemortException {
        innerStore.close();
    }

}
