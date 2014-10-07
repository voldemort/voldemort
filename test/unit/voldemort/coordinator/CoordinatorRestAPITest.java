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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.rest.RestMessageHeaders;
import voldemort.rest.RestUtils;
import voldemort.rest.coordinator.CoordinatorProxyService;
import voldemort.rest.coordinator.config.CoordinatorConfig;
import voldemort.rest.coordinator.config.FileBasedStoreClientConfigService;
import voldemort.rest.coordinator.config.StoreClientConfigService;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;

public class CoordinatorRestAPITest {

    private VoldemortServer[] servers;
    public static String socketUrl = "";
    private static final String STORE_NAME = "slow-store-test";
    private static final String STORES_XML = "test/common/voldemort/config/single-slow-store.xml";
    private static final String FAT_CLIENT_CONFIG_FILE_PATH_ORIGINAL = "test/common/voldemort/config/fat-client-config.avro";
    private static final String FAT_CLIENT_CONFIG_FILE_PATH = "/tmp/fat-client-config.avro";
    private final SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                        10000,
                                                                                        100000,
                                                                                        32 * 1024);
    private CoordinatorProxyService coordinator = null;
    private final String coordinatorURL = "http://localhost:8080";

    private class TestVersionedValue {

        private String value;
        private VectorClock vc;

        public TestVersionedValue(String val, VectorClock vc) {
            this.setValue(val);
            this.setVc(vc);
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public VectorClock getVc() {
            return vc;
        }

        public void setVc(VectorClock vc) {
            this.vc = vc;
        }
    }

    @Before
    public void setUp() throws Exception {
        int numServers = 1;
        servers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0, 2, 4, 6, 1, 3, 5, 7 } };
        Properties props = new Properties();
        props.setProperty("storage.configs",
                          "voldemort.store.bdb.BdbStorageConfiguration,voldemort.store.slow.SlowStorageConfiguration");
        props.setProperty("testing.slow.queueing.get.ms", "4");
        props.setProperty("testing.slow.queueing.getall.ms", "4");
        props.setProperty("testing.slow.queueing.put.ms", "4");
        props.setProperty("testing.slow.queueing.delete.ms", "4");

        ServerTestUtils.startVoldemortCluster(numServers,
                                              servers,
                                              partitionMap,
                                              socketStoreFactory,
                                              true, // useNio
                                              null,
                                              STORES_XML,
                                              props);

        // create a copy of the config file in /tmp and work on that
        File src = new File(FAT_CLIENT_CONFIG_FILE_PATH_ORIGINAL);
        File dest = new File(FAT_CLIENT_CONFIG_FILE_PATH);
        FileUtils.copyFile(src, dest);

        CoordinatorConfig config = new CoordinatorConfig();
        List<String> bootstrapUrls = new ArrayList<String>();
        socketUrl = servers[0].getIdentityNode().getSocketUrl().toString();
        bootstrapUrls.add(socketUrl);

        System.out.println("\n\n************************ Starting the Coordinator *************************");

        config.setBootstrapURLs(bootstrapUrls);
        config.setFatClientConfigPath(FAT_CLIENT_CONFIG_FILE_PATH);
        StoreClientConfigService storeClientConfigs = null;
        switch(config.getFatClientConfigSource()) {
            case FILE:
                storeClientConfigs = new FileBasedStoreClientConfigService(config);
                break;
            case ZOOKEEPER:
                throw new UnsupportedOperationException("Zookeeper-based configs are not implemented yet!");
            default:
                storeClientConfigs = null;
        }
        this.coordinator = new CoordinatorProxyService(config, storeClientConfigs);
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
        // clean up the temporary file created in set up
        File fatClientConfigFile = new File(FAT_CLIENT_CONFIG_FILE_PATH);
        fatClientConfigFile.delete();
    }

    public static enum ValueType {
        ALPHA,
        ALPHANUMERIC,
        NUMERIC
    }

    public static String generateRandomString(int length, ValueType type) {

        StringBuffer buffer = new StringBuffer();
        String characters = "";

        switch(type) {

            case ALPHA:
                characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
                break;

            case ALPHANUMERIC:
                characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
                break;

            case NUMERIC:
                characters = "1234567890";
                break;
        }

        int charactersLength = characters.length();

        for(int i = 0; i < length; i++) {
            double index = Math.random() * charactersLength;
            buffer.append(characters.charAt((int) index));
        }
        return buffer.toString();
    }

    private VectorClock doPut(String key, String payload, VectorClock vc) {
        return doPut(key, payload, vc, null);
    }

    private VectorClock doPut(String key,
                              String payload,
                              VectorClock vc,
                              Map<String, Object> options) {
        VectorClock successfulPutVC = null;
        int expectedResponseCode = 201;
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
            conn.setRequestProperty(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS, "1000");
            conn.setRequestProperty(RestMessageHeaders.X_VOLD_REQUEST_ORIGIN_TIME_MS,
                                    Long.toString(System.currentTimeMillis()));

            // options
            if(options != null) {
                if(options.get("timeout") != null && options.get("timeout") instanceof String) {
                    conn.setRequestProperty(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS,
                                            (String) options.get("timeout"));
                }
                if(options.get("responseCode") != null
                   && options.get("responseCode") instanceof Integer) {
                    expectedResponseCode = (Integer) options.get("responseCode");
                }
            }

            if(vc != null) {
                String eTag = RestUtils.getSerializedVectorClock(vc);
                conn.setRequestProperty("ETag", eTag);
            }

            // Write the payload
            OutputStream out = conn.getOutputStream();
            out.write(payload.getBytes());
            out.close();

            // Check for the right response code

            if(conn.getResponseCode() != expectedResponseCode) {
                System.err.println("Illegal response during PUT : " + conn.getResponseMessage());
                fail("Incorrect response received for a HTTP put request :"
                     + conn.getResponseCode());
            }

        } catch(Exception e) {
            e.printStackTrace();
            fail("Error in sending the REST request");
        }

        return successfulPutVC;
    }

    private boolean doDelete(String key) {
        return doDelete(key, null);
    }

    private boolean doDelete(String key, Map<String, Object> options) {
        int expectedResponseCode = 204;
        try {

            // Create the right URL and Http connection
            HttpURLConnection conn = null;
            String base64Key = new String(Base64.encodeBase64(key.getBytes()));
            URL url = new URL(this.coordinatorURL + "/" + STORE_NAME + "/" + base64Key);
            conn = (HttpURLConnection) url.openConnection();

            // Set the right headers
            conn.setRequestMethod("DELETE");
            conn.setDoInput(true);
            conn.setRequestProperty(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS, "1000");
            conn.setRequestProperty(RestMessageHeaders.X_VOLD_REQUEST_ORIGIN_TIME_MS,
                                    Long.toString(System.currentTimeMillis()));

            // options
            if(options != null) {
                if(options.get("timeout") != null && options.get("timeout") instanceof String) {
                    conn.setRequestProperty(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS,
                                            (String) options.get("timeout"));
                }
                if(options.get("responseCode") != null
                   && options.get("responseCode") instanceof Integer) {
                    expectedResponseCode = (Integer) options.get("responseCode");
                }
            }

            // Check for the right response code
            if(conn.getResponseCode() != expectedResponseCode) {
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

    private TestVersionedValue doGet(String key) {
        return doGet(key, null);
    }

    private TestVersionedValue doGet(String key, Map<String, Object> options) {
        HttpURLConnection conn = null;
        String response = null;
        TestVersionedValue responseObj = null;
        int expectedResponseCode = 200;
        try {

            // Create the right URL and Http connection
            String base64Key = new String(Base64.encodeBase64(key.getBytes()));
            URL url = new URL(this.coordinatorURL + "/" + STORE_NAME + "/" + base64Key);
            conn = (HttpURLConnection) url.openConnection();

            // Set the right headers
            conn.setRequestMethod("GET");
            conn.setDoInput(true);
            conn.setRequestProperty(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS, "1000");
            conn.setRequestProperty(RestMessageHeaders.X_VOLD_REQUEST_ORIGIN_TIME_MS,
                                    Long.toString(System.currentTimeMillis()));

            // options
            if(options != null) {
                if(options.get("timeout") != null && options.get("timeout") instanceof String) {
                    conn.setRequestProperty(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS,
                                            (String) options.get("timeout"));
                }
                if(options.get("responseCode") != null
                   && options.get("responseCode") instanceof Integer) {
                    expectedResponseCode = (Integer) options.get("responseCode");
                }
            }

            // Check for the right response code
            if(conn.getResponseCode() != expectedResponseCode) {
                System.err.println("Illegal response during GET : " + conn.getResponseMessage());
                fail("Incorrect response received for a HTTP GET request :"
                     + conn.getResponseCode());
            }

            if(conn.getResponseCode() == 404 || conn.getResponseCode() == 408) {
                return null;
            }

            // Buffer the result into a string
            ByteArrayDataSource ds = new ByteArrayDataSource(conn.getInputStream(),
                                                             "multipart/mixed");
            MimeMultipart mp = new MimeMultipart(ds);
            assertEquals("The number of body parts expected is not 1", 1, mp.getCount());

            MimeBodyPart part = (MimeBodyPart) mp.getBodyPart(0);
            VectorClock vc = RestUtils.deserializeVectorClock(part.getHeader(RestMessageHeaders.X_VOLD_VECTOR_CLOCK)[0]);
            int contentLength = Integer.parseInt(part.getHeader(RestMessageHeaders.CONTENT_LENGTH)[0]);
            byte[] bodyPartBytes = new byte[contentLength];

            part.getInputStream().read(bodyPartBytes);
            response = new String(bodyPartBytes);

            responseObj = new TestVersionedValue(response, vc);

        } catch(Exception e) {
            e.printStackTrace();
            fail("Error in sending the REST request");
        } finally {
            if(conn != null) {
                conn.disconnect();
            }
        }
        return responseObj;
    }

    @Test
    public void testReadAfterWrite() {
        String key = "Which_Imperial_IPA_do_I_want_to_drink";
        String payload = "Pliny the Younger";

        // 1. Do a put
        doPut(key, payload, null);

        // 2. Do a get on the same key
        TestVersionedValue response = doGet(key);
        if(response == null) {
            fail("key does not exist after a put. ");
        }

        System.out.println("Received value: " + response.getValue());
        if(!response.getValue().equals(payload)) {
            fail("Received value is incorrect ! Expected : " + payload + " but got : "
                 + response.getValue());
        }
    }

    @Test
    public void testDelete() {
        String key = "Which_sour_beer_do_I_want_to_drink";
        String payload = "Duchesse De Bourgogne";
        Map<String, Object> options = new HashMap<String, Object>();

        // 1. Do a put
        doPut(key, payload, null);

        // 2. Do a get on the same key
        TestVersionedValue response = doGet(key);
        if(response == null) {
            fail("key does not exist after a put. ");
        }
        System.out.println("Received value: " + response.getValue());
        if(!response.getValue().equals(payload)) {
            fail("Received value is incorrect ! Expected : " + payload + " but got : "
                 + response.getValue());
        }

        // 3. Do a delete
        boolean isDeleted = doDelete(key);
        if(!isDeleted) {
            fail("Could not delete the key. Error !");
        }

        // 4. Do a get on the same key : this should fail
        options.put("responseCode", 404);
        response = doGet(key, options);
        if(response != null) {
            fail("key still exists after deletion. ");
        }
    }

    @Test
    public void testVersionedPut() {
        String key = "Which_Porter_do_I_want_to_drink";
        String payload = "Founders Porter";
        String newPayload = "Samuel Smith Taddy Porter";

        // 1. Do a put
        doPut(key, payload, null);

        // 2. Do a get on the same key
        TestVersionedValue response = doGet(key, null);
        if(response == null) {
            fail("key does not exist after a put. ");
        }
        System.out.println("Received value: " + response.getValue());

        // 3. Do a versioned put based on the version received previously
        doPut(key, newPayload, response.getVc());

        // 4. Do a get again on the same key
        TestVersionedValue newResponse = doGet(key);
        if(newResponse == null) {
            fail("key does not exist after the versioned put. ");
        }
        assertEquals("Returned response does not have a higer version",
                     Occurred.AFTER,
                     newResponse.getVc().compare(response.getVc()));
        assertEquals("Returned response does not have a higer version",
                     Occurred.BEFORE,
                     response.getVc().compare(newResponse.getVc()));

        System.out.println("Received value after the Versioned put: " + newResponse.getValue());
        if(!newResponse.getValue().equals(newPayload)) {
            fail("Received value is incorrect ! Expected : " + newPayload + " but got : "
                 + newResponse.getValue());
        }
    }

    @Test
    public void testLargeValueSizeVersionedPut() {
        String key = "amigo";
        String payload = generateRandomString(new CoordinatorConfig().getHttpMessageDecoderMaxChunkSize() * 10,
                                              ValueType.ALPHA);
        String newPayload = generateRandomString(new CoordinatorConfig().getHttpMessageDecoderMaxChunkSize() * 10,
                                                 ValueType.ALPHA);

        // 1. Do a put
        doPut(key, payload, null);

        // 2. Do a get on the same key
        TestVersionedValue response = doGet(key, null);
        if(response == null) {
            fail("key does not exist after a put. ");
        }
        System.out.println("Received value: " + response.getValue());

        // 3. Do a versioned put based on the version received previously
        doPut(key, newPayload, response.getVc());

        // 4. Do a get again on the same key
        TestVersionedValue newResponse = doGet(key);
        if(newResponse == null) {
            fail("key does not exist after the versioned put. ");
        }
        assertEquals("Returned response does not have a higer version",
                     Occurred.AFTER,
                     newResponse.getVc().compare(response.getVc()));
        assertEquals("Returned response does not have a higer version",
                     Occurred.BEFORE,
                     response.getVc().compare(newResponse.getVc()));

        System.out.println("Received value after the Versioned put: " + newResponse.getValue());
        if(!newResponse.getValue().equals(newPayload)) {
            fail("Received value is incorrect ! Expected : " + newPayload + " but got : "
                 + newResponse.getValue());
        }
    }

    @Test
    public void testWriteWithTimeout() {
        String key = "Which_Imperial_IPA_do_I_want_to_drink";
        String payload = "Pliny the Younger";
        Map<String, Object> options = new HashMap<String, Object>();

        // 1. Do a put (timeout)
        options.put("timeout", "1");
        options.put("responseCode", 408);
        doPut(key, payload, null, options);

        // 2. Do a get on the same key
        options.clear();
        options.put("responseCode", 404);
        TestVersionedValue response = doGet(key, options);
        if(response != null) {
            fail("key should not exist after a put. ");
        }

        // 3. Do a put
        doPut(key, payload, null);

        // 4. Do a get on the same key with timeout
        options.clear();
        options.put("timeout", "1");
        options.put("responseCode", 408);
        response = doGet(key, options);

        // 5. Do a get on the same key
        response = doGet(key);
        if(response == null) {
            fail("key does not exist after a put. ");
        }
    }
}
