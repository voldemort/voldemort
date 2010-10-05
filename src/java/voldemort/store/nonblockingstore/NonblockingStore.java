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

import java.util.Map;

import voldemort.VoldemortException;
import voldemort.store.Store;
import voldemort.store.routed.RoutedStore;
import voldemort.store.socket.SocketStore;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A NonblockingStore mimics the {@link Store} interface but instead of blocking
 * for the request to complete, it simply submits it for later processing and
 * returns immediately. When the request is processed, the provided
 * {@link NonblockingStoreCallback callback} is invoked in order to provide
 * interested parties with the results of the request.
 * 
 * <p/>
 * 
 * At this point, the NonblockingStore is used from within the
 * {@link RoutedStore} in order to provide asynchronous processing against
 * multiple remote stores in parallel using a single thread approach.
 * 
 * <p/>
 * 
 * There are two main implementations:
 * 
 * <ol>
 * <li>{@link ThreadPoolBasedNonblockingStoreImpl} wraps a "blocking"
 * {@link Store} inside a thread pool to provide a submit-and-return style of
 * asynchronous request/response. This is useful for the case where the Store
 * implementation is not based on {@link SocketStore} and as such cannot use
 * NIO-based non-blocking networking.
 * <li>{@link SocketStore} uses NIO to submit a request to the networking layer
 * which will process <b>all</b> requests to remote servers in a dedicated
 * thread.
 * </ol>
 * 
 * @see Store
 * @see ThreadPoolBasedNonblockingStoreImpl
 * @see SocketStore
 * @see NonblockingStoreCallback
 * @see RoutedStore
 */

public interface NonblockingStore {

    public void submitGetRequest(ByteArray key,
                                 byte[] transforms,
                                 NonblockingStoreCallback callback,
                                 long timeoutMs);

    public void submitGetAllRequest(Iterable<ByteArray> keys,
                                    Map<ByteArray, byte[]> transforms,
                                    NonblockingStoreCallback callback,
                                    long timeoutMs);

    public void submitGetVersionsRequest(ByteArray key,
                                         NonblockingStoreCallback callback,
                                         long timeoutMs);

    public void submitPutRequest(ByteArray key,
                                 Versioned<byte[]> value,
                                 byte[] transforms,
                                 NonblockingStoreCallback callback,
                                 long timeoutMs);

    public void submitDeleteRequest(ByteArray key,
                                    Version version,
                                    NonblockingStoreCallback callback,
                                    long timeoutMs);

    public void close() throws VoldemortException;

}