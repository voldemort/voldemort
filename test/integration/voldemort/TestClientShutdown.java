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

package voldemort;

import voldemort.client.ClientConfig;
import voldemort.client.DefaultStoreClient;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClientFactory;
import voldemort.utils.Utils;

/**
 * Code to test that the client shuts down gracefully. If this program doesn't
 * exit immediately that is bad
 * 
 * @author jay
 * 
 */
public class TestClientShutdown {

    public static void main(String[] args) throws Exception {
        if(args.length < 2 || args.length > 3)
            Utils.croak("USAGE: java VoldemortClientShell store_name bootstrap_url [command_file]");

        String storeName = args[0];
        String bootstrapUrl = args[1];

        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
        DefaultStoreClient<Object, Object> client = null;
        try {
            client = (DefaultStoreClient<Object, Object>) factory.getStoreClient(storeName);
        } catch(Exception e) {
            Utils.croak("Could not connect to server: " + e.getMessage());
        }

        System.out.println("Established connection to " + storeName + " via " + bootstrapUrl);

        client.get("hello");

        System.out.println("Got value");
    }
}
