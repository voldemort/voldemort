/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort.restclient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import voldemort.client.StoreClient;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Temporary test until we tie in the Rest Client with the existing StoreClient
 * unit tests.
 * 
 */
public class SampleRESTClient {

    public static void main(String[] args) {

        // Create the client
        RESTClientConfig config = new RESTClientConfig();
        config.setHttpBootstrapURL("http://localhost:8080")
              .setTimeoutMs(1500, TimeUnit.MILLISECONDS)
              .setMaxR2PoolSize(100);

        RESTClientFactory factory = new RESTClientFactory(config);
        StoreClient<String, String> clientStore = factory.getStoreClient("test");

        try {

            // Sample put
            System.out.println("First valid put");
            clientStore.put("a", "Howdy!!!!");
            System.out.println("Second valid put");
            clientStore.put("b", "Partner!!!!");

            // Do a sample get operation:
            Versioned<String> versionedValue = clientStore.get("a");
            System.out.println("Received response : " + versionedValue);
            Version obsoleteVersion = ((VectorClock) versionedValue.getVersion()).clone();

            // Do a versioned put operation:
            System.out.println("First versioned put");
            versionedValue.setObject("New Value !!!");
            System.err.println("************* original version : " + versionedValue.getVersion());
            Version putVersion = clientStore.put("a", versionedValue);
            System.err.println("************* Updated version : " + putVersion);

            // Obsolete version put
            System.out.println("Obsolete put");
            Versioned<String> obsoleteVersionedValue = new Versioned<String>("Obsolete value",
                                                                             obsoleteVersion);
            try {
                clientStore.put("a", obsoleteVersionedValue);
                System.err.println(" **************** Should not reach this point **************** ");
            } catch(Exception e) {
                e.printStackTrace();
            }

            // Do a get again on the last versioned put operation:
            versionedValue = clientStore.get("a");
            System.out.println("Received response on the versioned put: " + versionedValue);

            System.out.println("Versioned put based on the last put ");
            Versioned<String> newVersionedPut = new Versioned<String>("Yet another value !!!",
                                                                      putVersion);
            clientStore.put("a", newVersionedPut);

            // Do a get again on the last versioned put operation:
            versionedValue = clientStore.get("a");
            System.out.println("Received response on the (second) versioned put: " + versionedValue);

            List<String> keyList = new ArrayList<String>();
            keyList.add("a");
            keyList.add("b");
            System.out.println("Received response : " + clientStore.getAll(keyList));

        } finally {
            factory.close();
        }
    }
}
