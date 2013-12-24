/**
 * Copyright 2013 LinkedIn, Inc
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

import voldemort.client.ClientConfig;
import voldemort.cluster.Node;
import voldemort.server.RequestRoutingType;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

public class TestSocketStoreFactory extends ClientRequestExecutorPool {
    static final Integer DEFAULT_MAX_CONNECTIONS_PER_NODE = 2;
    static final Integer DEFAULT_CONNECTION_TIMEOUT_MS = 10000;
    static final Integer DEFAULT_SO_TIMEOUT_MS = 100000;
    static final Integer DEFAULT_SOCKET_BUFFER_SIZE = 1024;
    static final ClientConfig DEFAULT_CLIENT_CONFIG = new ClientConfig();


    public TestSocketStoreFactory() {
        super(DEFAULT_SELECTORS,
                DEFAULT_MAX_CONNECTIONS_PER_NODE,
                DEFAULT_CONNECTION_TIMEOUT_MS,
                DEFAULT_SO_TIMEOUT_MS,
                DEFAULT_SOCKET_BUFFER_SIZE,
                DEFAULT_SOCKET_KEEP_ALIVE,
                DEFAULT_JMX_ENABLED,
                DEFAULT_JMX_ID);
    }

    public SocketStore createSocketStore(Node node, String storeName) {
        return create(storeName, node.getHost(), node.getSocketPort(), DEFAULT_CLIENT_CONFIG.getRequestFormatType(), RequestRoutingType.NORMAL);
    }
}
