/*
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

package voldemort.coordinator;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.codec.binary.Base64;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

public class CoordinatorRestAPITest {

    private VoldemortServer[] servers;
    private Cluster cluster;
    public static String socketUrl = "";
    private static final String STORE_NAME = "test";
    private static final String STORES_XML = "test/common/voldemort/config/single-store.xml";
    private static final String FAT_CLIENT_CONFIG_FILE_PATH = "test/common/voldemort/config/fat-client-config.avro";
    private final SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                        10000,
                                                                                        100000,
                                                                                        32 * 1024);
    private CoordinatorService coordinator = null;
    private final String coordinatorURL = "http://localhost:8080";

    @Before
    public void setUp() throws Exception {
        int numServers = 1;
        servers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0, 2, 4, 6, 1, 3, 5, 7 } };
        Properties props = new Properties();
        props.setProperty("storage.configs",
                          "voldemort.store.bdb.BdbStorageConfiguration,voldemort.store.slow.SlowStorageConfiguration");

        cluster = ServerTestUtils.startVoldemortCluster(numServers,
                                                        servers,
                                                        partitionMap,
                                                        socketStoreFactory,
                                                        true, // useNio
                                                        null,
                                                        STORES_XML,
                                                        props);

        CoordinatorConfig config = new CoordinatorConfig();
        List<String> bootstrapUrls = new ArrayList<String>();
        socketUrl = servers[0].getIdentityNode().getSocketUrl().toString();
        bootstrapUrls.add(socketUrl);

        System.out.println("\n\n************************ Starting the Coordinator *************************");

        config.setBootstrapURLs(bootstrapUrls);
        config.setFatClientConfigPath(FAT_CLIENT_CONFIG_FILE_PATH);

        this.coordinator = new CoordinatorService(config);
        if(!this.coordinator.isStarted()) {
            this.coordinator.start();
        }
    }

    @After
    public void tearDown() throws Exception {
        if(this.socketStoreFactory != null) {
            this.socketStoreFactory.close();
        }

        if(this.coordinator != null && this.coordinator.isStarted()) {
            this.coordinator.stop();
        }
    }

    private void doPut(String key, String payload) {
        try {
            // Create the right URL and Http connection
            HttpURLConnection conn = null;
            String base64Key = new String(Base64.encodeBase64(key.getBytes()));
            URL url = new URL(this.coordinatorURL + "/" + STORE_NAME + "/" + base64Key);
            conn = (HttpURLConnection) url.openConnection();

            // Set the right headers
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setRequestProperty("Content-Type", "binary");
            conn.setRequestProperty("Content-Length", "" + payload.length());
            conn.setRequestProperty(VoldemortHttpRequestHandler.X_VOLD_REQUEST_TIMEOUT_MS, "1000");

            // Write the payload
            OutputStream out = conn.getOutputStream();
            out.write(payload.getBytes());
            out.close();

            // Check for the right response code
            if(conn.getResponseCode() != 201) {
                System.err.println("Illegal response during PUT : " + conn.getResponseMessage());
                fail("Incorrect response received for a HTTP put request :"
                     + conn.getResponseCode());
            }
        } catch(Exception e) {
            e.printStackTrace();
            fail("Error in sending the REST request");
        }
    }

    private boolean doDelete(String key) {
        try {

            // Create the right URL and Http connection
            HttpURLConnection conn = null;
            String base64Key = new String(Base64.encodeBase64(key.getBytes()));
            URL url = new URL(this.coordinatorURL + "/" + STORE_NAME + "/" + base64Key);
            conn = (HttpURLConnection) url.openConnection();

            // Set the right headers
            conn.setRequestMethod("DELETE");
            conn.setDoInput(true);
            conn.setRequestProperty(VoldemortHttpRequestHandler.X_VOLD_REQUEST_TIMEOUT_MS, "1000");

            // Check for the right response code
            if(conn.getResponseCode() != 204) {
                System.err.println("Illegal response during DELETE : " + conn.getResponseMessage());
                fail("Incorrect response received for a HTTP put request :"
                     + conn.getResponseCode());
            } else {
                return true;
            }

        } catch(Exception e) {
            e.printStackTrace();
            fail("Error in sending the REST request");
        }

        return false;
    }

    private String doGet(String key) {
        String response = null;
        try {

            // Create the right URL and Http connection
            HttpURLConnection conn = null;
            String base64Key = new String(Base64.encodeBase64(key.getBytes()));
            URL url = new URL(this.coordinatorURL + "/" + STORE_NAME + "/" + base64Key);
            conn = (HttpURLConnection) url.openConnection();

            // Set the right headers
            conn.setRequestMethod("GET");
            conn.setDoInput(true);
            conn.setRequestProperty(VoldemortHttpRequestHandler.X_VOLD_REQUEST_TIMEOUT_MS, "1000");

            if(conn.getResponseCode() == 404) {
                return null;
            }

            // Check for the right response code
            if(conn.getResponseCode() != 200) {
                System.err.println("Illegal response during GET : " + conn.getResponseMessage());
                fail("Incorrect response received for a HTTP put request :"
                     + conn.getResponseCode());
            }

            // Buffer the result into a string
            BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String line;
            while((line = rd.readLine()) != null) {
                sb.append(line);
            }
            rd.close();
            conn.disconnect();

            response = sb.toString();

        } catch(Exception e) {
            e.printStackTrace();
            fail("Error in sending the REST request");
        }

        return response;
    }

    @Test
    public void testRESTReadAfterWrite() {
        String key = "Which_Imperial_IPA_do_I_want_to_drink";
        String payload = "Pliny the Younger";

        // 1. Do a put
        doPut(key, payload);

        // 2. Do a get on the same key
        String response = doGet(key);
        if(response == null) {
            fail("key does not exist after a put. ");
        }
        System.out.println("Received value: " + response);
        if(!response.equals(payload)) {
            fail("Received value is incorrect ! Expected : " + payload + " but got : " + response);
        }
    }

    @Test
    public void testDelete() {
        String key = "Which_sour_beer_do_I_want_to_drink";
        String payload = "Duchesse De Bourgogne";

        // 1. Do a put
        doPut(key, payload);

        // 2. Do a get on the same key
        String response = doGet(key);
        if(response == null) {
            fail("key does not exist after a put. ");
        }
        System.out.println("Received value: " + response);
        if(!response.equals(payload)) {
            fail("Received value is incorrect ! Expected : " + payload + " but got : " + response);
        }

        // 3. Do a delete
        boolean isDeleted = doDelete(key);
        if(!isDeleted) {
            fail("Could not delete the key. Error !");
        }

        // 4. Do a get on the same key : this should fail
        response = doGet(key);
        if(response != null) {
            fail("key still exists after deletion. ");
        }
    }

}
