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

import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.client.DaemonThreadFactory;
import voldemort.client.AbstractStoreClientFactory;
import voldemort.versioning.Versioned;
import voldemort.serialization.mongodb.MongoDBSerializationFactory;
import org.mongodb.driver.ts.Doc;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;

public class MongoDBClient {

    public static void main(String[] args) {

        // In real life this stuff would get wired in
        int numThreads = 10;
        int maxQueuedRequests = 10;
        int maxConnectionsPerNode = 10;
        int maxTotalConnections = 100;
        String bootstrapUrl = "tcp://localhost:6666";

//        StoreClientFactory factory = new SocketStoreClientFactory(numThreads,
//                                                                  numThreads,
//                                                                  maxQueuedRequests,
//                                                                  maxConnectionsPerNode,
//                                                                  maxTotalConnections,
//                                                                  bootstrapUrl);


        StoreClientFactory factory = new SocketStoreClientFactory(
                new ThreadPoolExecutor(numThreads,
                                    numThreads,
                                    10000L,
                                    TimeUnit.MILLISECONDS,
                                    new LinkedBlockingQueue<Runnable>(maxQueuedRequests),
                                    new DaemonThreadFactory("voldemort-client-thread-"),
                                    new ThreadPoolExecutor.CallerRunsPolicy()),
                                maxConnectionsPerNode,
                                maxTotalConnections,
                                5000,
                                AbstractStoreClientFactory.DEFAULT_ROUTING_TIMEOUT_MS,
                                AbstractStoreClientFactory.DEFAULT_NODE_BANNAGE_MS,
                                new MongoDBSerializationFactory(),
                                bootstrapUrl);


        StoreClient<String , Doc> client = factory.getStoreClient("test");

        Versioned<Doc> v = client.get("key");

        if (v == null) {
            Doc d = new Doc("name", "geir");
            d.add("x", 1);

            v  = new Versioned<Doc>(d);
        }

        // update the value
        client.put("key", v);

        v = client.get("key");

        System.out.println("value : " + v.getValue());
        System.out.println("clock : " + v.getVersion());

    }

}
