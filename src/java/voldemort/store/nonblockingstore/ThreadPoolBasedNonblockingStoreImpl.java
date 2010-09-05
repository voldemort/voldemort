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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

import com.google.common.collect.Lists;

public class ThreadPoolBasedNonblockingStoreImpl implements NonblockingStore {

    private final ExecutorService executor;

    private final Store<ByteArray, byte[]> innerStore;

    private final Logger logger = Logger.getLogger(getClass());

    public ThreadPoolBasedNonblockingStoreImpl(ExecutorService executor,
                                               Store<ByteArray, byte[]> innerStore) {
        this.executor = Utils.notNull(executor);
        this.innerStore = Utils.notNull(innerStore);
    }

    public void submitGetAllRequest(final Iterable<ByteArray> keys,
                                    final NonblockingStoreCallback callback,
                                    long timeoutMs) {
        submit(new StoreRequest<Map<ByteArray, List<Versioned<byte[]>>>>() {

            public Map<ByteArray, List<Versioned<byte[]>>> request(Store<ByteArray, byte[]> store) {
                return innerStore.getAll(keys);
            }

        }, callback, timeoutMs, "get all");
    }

    public void submitGetRequest(final ByteArray key,
                                 NonblockingStoreCallback callback,
                                 long timeoutMs) {
        submit(new StoreRequest<List<Versioned<byte[]>>>() {

            public List<Versioned<byte[]>> request(Store<ByteArray, byte[]> store) {
                return innerStore.get(key);
            }

        }, callback, timeoutMs, "get");
    }

    public void submitGetVersionsRequest(final ByteArray key,
                                         NonblockingStoreCallback callback,
                                         long timeoutMs) {
        submit(new StoreRequest<List<Version>>() {

            public List<Version> request(Store<ByteArray, byte[]> store) {
                return innerStore.getVersions(key);
            }

        }, callback, timeoutMs, "submit");
    }

    public void submitPutRequest(final ByteArray key,
                                 final Versioned<byte[]> value,
                                 NonblockingStoreCallback callback,
                                 long timeoutMs) {
        submit(new StoreRequest<Void>() {

            public Void request(Store<ByteArray, byte[]> store) {
                innerStore.put(key, value);
                return null;
            }

        }, callback, timeoutMs, "put");
    }

    public void submitDeleteRequest(final ByteArray key,
                                    final Version version,
                                    NonblockingStoreCallback callback,
                                    long timeoutMs) {
        submit(new StoreRequest<Boolean>() {

            public Boolean request(Store<ByteArray, byte[]> store) {
                return innerStore.delete(key, version);
            }

        }, callback, timeoutMs, "delete");
    }

    @SuppressWarnings("unchecked")
    private void submit(final StoreRequest<?> request,
                        final NonblockingStoreCallback callback,
                        long timeoutMs,
                        String operationName) {
        final AtomicBoolean isRequestComplete = new AtomicBoolean(false);
        final long start = System.nanoTime();

        Callable<Object> callable = new Callable<Object>() {

            public Object call() {
                Object result = null;

                try {
                    result = request.request(innerStore);

                    if(isRequestComplete.compareAndSet(false, true)) {
                        if(callback != null)
                            callback.requestComplete(result, (System.nanoTime() - start)
                                                             / Time.NS_PER_MS);
                    }
                } catch(Exception e) {
                    if(isRequestComplete.compareAndSet(false, true)) {
                        if(callback != null)
                            callback.requestComplete(e, (System.nanoTime() - start)
                                                        / Time.NS_PER_MS);
                    }
                }

                return result;
            }

        };

        Collection<Callable<Object>> tasks = Lists.newArrayList(callable);

        try {
            executor.invokeAll(tasks, timeoutMs, TimeUnit.MILLISECONDS);
        } catch(InterruptedException e) {
            if(isRequestComplete.compareAndSet(false, true)) {
                if(callback != null) {
                    UnreachableStoreException ex = new UnreachableStoreException("Failure in "
                                                                                         + operationName
                                                                                         + ": "
                                                                                         + e.getMessage(),
                                                                                 e);
                    callback.requestComplete(ex, (System.nanoTime() - start) / Time.NS_PER_MS);
                }
            }

            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e);
        }

    }

    public void close() throws VoldemortException {
        innerStore.close();
    }

}
