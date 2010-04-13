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

package voldemort.store.socket;

import voldemort.client.protocol.RequestFormatType;
import voldemort.server.RequestRoutingType;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

/**
 * SocketStoreFactory manages the creation of socket-based Store instances.
 * Under the covers there are often shared resources (socket pools, etc.) that
 * are initialized (and destroyed) in a single section of code rather than
 * distributed over multiple methods/classes/packages.
 */

public interface SocketStoreFactory {

    public Store<ByteArray, byte[]> create(String storeName,
                                           String hostName,
                                           int port,
                                           RequestFormatType requestFormatType,
                                           RequestRoutingType requestRoutingType);

    public void close();

    public void close(SocketDestination destination);

}
