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
import java.util.Properties;

import voldemort.client.ClientConfig;
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
        Properties props = new Properties();
        props.setProperty(ClientConfig.BOOTSTRAP_URLS_PROPERTY, "http://localhost:8080");
        props.setProperty(ClientConfig.ROUTING_TIMEOUT_MS_PROPERTY, "1500");

        RESTClientFactoryConfig mainConfig = new RESTClientFactoryConfig(props, null);
        RESTClientFactory factory = new RESTClientFactory(mainConfig);
        StoreClient<String, String> storeClient = factory.getStoreClient("test");

        try {

            // Sample put
            System.out.println("First valid put");
            storeClient.put("a", "Howdy!!!!");
            System.out.println("Second valid put");
            storeClient.put("b", "Partner!!!!");

            // Do a sample get operation:
            Versioned<String> versionedValue = storeClient.get("a");
            System.out.println("Received response : " + versionedValue);
            Version obsoleteVersion = ((VectorClock) versionedValue.getVersion()).clone();

            // Do a versioned put operation:
            System.out.println("First versioned put");
            versionedValue.setObject("New Value !!!");
            System.out.println("************* original version : " + versionedValue.getVersion());
            Version putVersion = storeClient.put("a", versionedValue);
            System.out.println("************* Updated version : " + putVersion);

            // Obsolete version put
            System.out.println("Obsolete put");
            Versioned<String> obsoleteVersionedValue = new Versioned<String>("Obsolete value",
                                                                             obsoleteVersion);
            try {
                storeClient.put("a", obsoleteVersionedValue);
                System.err.println(" **************** Should not reach this point **************** ");
            } catch(Exception e) {
                System.out.println("Exception occurred as expected: " + e.getMessage());
            }

            // Do a get again on the last versioned put operation:
            versionedValue = storeClient.get("a");
            System.out.println("Received response on the versioned put: " + versionedValue);

            System.out.println("Versioned put based on the last put ");
            Versioned<String> newVersionedPut = new Versioned<String>("Yet another value !!!",
                                                                      putVersion);
            storeClient.put("a", newVersionedPut);

            // Do a get again on the last versioned put operation:
            versionedValue = storeClient.get("a");
            System.out.println("Received response on the (second) versioned put: " + versionedValue);

            List<String> keyList = new ArrayList<String>();
            keyList.add("a");
            keyList.add("b");
            System.out.println("Received response : " + storeClient.getAll(keyList));

        } finally {
            factory.close();
        }
    }
}
