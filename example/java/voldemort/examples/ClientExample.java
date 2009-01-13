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

package voldemort.examples;

import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.versioning.Versioned;

public class ClientExample {

    public static void main(String[] args) {

        // In real life this stuff would get wired in
        int numThreads = 10;
        int maxQueuedRequests = 10;
        int maxConnectionsPerNode = 10;
        int maxTotalConnections = 100;
        String bootstrapUrl = "tcp://localhost:6666";
        StoreClientFactory factory = new SocketStoreClientFactory(numThreads,
                                                                  numThreads,
                                                                  maxQueuedRequests,
                                                                  maxConnectionsPerNode,
                                                                  maxTotalConnections,
                                                                  bootstrapUrl);

        StoreClient<String, String> client = factory.getStoreClient("my_store_name");

        // get the value
        Versioned<String> version = client.get("some_key");

        // modify the value
        version.setObject("new_value");

        // update the value
        client.put("some_key", version);
    }

}
