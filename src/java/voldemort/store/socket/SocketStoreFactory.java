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

/**
 * SocketStoreFactory manages the creation of {@link SocketStore} instances.
 * Under the covers there are often shared resources (socket pools, etc.) that
 * are initialized (and destroyed) in a single section of code rather than
 * distributed over multiple methods/classes/packages.
 */

public interface SocketStoreFactory {

    /**
     * Creates a new SocketStore using the specified store name, remote server,
     * format type and routing type.
     * 
     * @param storeName Name of store
     * @param hostName Host name of remote Voldemort node
     * @param port Port on which hostName is listening
     * @param requestFormatType {@link RequestFormatType}
     * @param requestRoutingType {@link RequestRoutingType}
     * 
     * @return New {@link SocketStore}
     */

    public SocketStore create(String storeName,
                              String hostName,
                              int port,
                              RequestFormatType requestFormatType,
                              RequestRoutingType requestRoutingType);

    /**
     * Closes the entire factory, which means that any shared resources used by
     * socket store implementations will be closed as well. It is therefore best
     * to close all {@link SocketStore} instances <b>first</b>, then close the
     * socket store factory.
     */

    public void close();

    /**
     * This closes the resources for a specific host, usually in response to an
     * error in communicating with that host.
     * 
     * @param destination {@link SocketDestination} representing the host name,
     *        port, etc. for a remote host
     */

    public void close(SocketDestination destination);

}
