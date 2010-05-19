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

package voldemort.performance;

import voldemort.client.StoreClient;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.utils.Props;

public class SemiLocalHttpStoreLoadTest extends AbstractLoadTestHarness {

    @Override
    public StoreClient<String, String, String> getStore(Props propsA, Props propsB)
            throws java.lang.Exception {
        System.out.println("Initializing master server.");
        VoldemortServer serverA = new VoldemortServer(new VoldemortConfig(propsA));
        System.out.println("Initializing slave server.");
        VoldemortServer serverB = new VoldemortServer(new VoldemortConfig(propsB));

        serverA.start();
        serverB.start();

        return null;
        // Clients.getSemiLocalRoutedHttpStoreClient(Collections.singletonList(serverA.getIdentityNode().getHttpUrl()),
        // serverA.getStoreConfiguration(),
        // "users",
        // new StringSerializer("UTF-8"),
        // serverA.getNodeId(),
        // 20);
    }

    public static void main(String[] args) throws Exception {
        new RemoteHttpStoreLoadTest().run(args);
    }

}
