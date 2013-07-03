package voldemort.server.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import voldemort.coordinator.CoordinatorUtils;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.versioning.VectorClock;

public class RestServerProtocolTests {

    private static String storeNameStr;
    private static String key1;
    private static String urlStr;
    private static String value1;
    private static String contentType;
    private static long timeOut, originTime;
    private static int routingType;
    private static VectorClock vectorClock;
    private static String eTag;
    private static VoldemortServer server;
    private static VoldemortConfig config;
    private static Logger logger = Logger.getLogger(RestServerProtocolTests.class);

    /*
     * TODO REST-Server: Hard coded storeName and urlStr. They must be retrieved
     * from config
     */

    @BeforeClass
    public static void oneTimeSetUp() {
        storeNameStr = "test";
        urlStr = "http://localhost:8081/";
        config = VoldemortConfig.loadFromVoldemortHome("config/single_node_cluster/");
        key1 = "The longest key ";
        vectorClock = new VectorClock();
        vectorClock.incrementVersion(config.getNodeId(), System.currentTimeMillis());
        eTag = CoordinatorUtils.getSerializedVectorClock(vectorClock);
        value1 = "The longest value";
        timeOut = 1000L;
        contentType = "text";
        originTime = System.currentTimeMillis();
        routingType = 2;
        server = new VoldemortServer(config);
        if(!server.isStarted())
            server.start();
        System.out.println("********************Starting REST Server********************");
    }

    @AfterClass
    public static void oneTimeCleanUp() {
        if(server != null && server.isStarted()) {
            server.stop();
        }
    }

    public HttpURLConnection doPut(String url,
                                   String key,
                                   String value,
                                   String storeName,
                                   String originTime,
                                   String timeOut,
                                   String routingType,
                                   String vectorClock,
                                   String ContentType,
                                   String contentLength) throws IOException {

        HttpURLConnection conn = createConnection(url,
                                                  key,
                                                  storeName,
                                                  originTime,
                                                  timeOut,
                                                  routingType,
                                                  "POST");
        conn.setDoOutput(true);
        if(vectorClock != null) {
            conn.setRequestProperty(RestMessageHeaders.X_VOLD_VECTOR_CLOCK, vectorClock);
        }
        if(ContentType != null) {
            conn.setRequestProperty("Content-Type", ContentType);
        }
        if(contentLength != null) {
            conn.setRequestProperty("Content-Length", contentLength);
        }

        if(value != null) {
            OutputStream out = conn.getOutputStream();
            out.write(value.getBytes());
            out.close();
        }
        return conn;
    }

    public HttpURLConnection doDelete(String url,
                                      String key,
                                      String storeName,
                                      String originTime,
                                      String timeOut,
                                      String routingType,
                                      String vectorClock) throws IOException {
        HttpURLConnection conn = createConnection(url,
                                                  key,
                                                  storeName,
                                                  originTime,
                                                  timeOut,
                                                  routingType,
                                                  "DELETE");
        if(vectorClock != null) {
            conn.setRequestProperty(RestMessageHeaders.X_VOLD_VECTOR_CLOCK, vectorClock);
        }
        return conn;

    }

    public HttpURLConnection doGet(String url,
                                   String key,
                                   String storeName,
                                   String originTime,
                                   String timeOut,
                                   String routingType) throws IOException {

        HttpURLConnection conn = createConnection(url,
                                                  key,
                                                  storeName,
                                                  originTime,
                                                  timeOut,
                                                  routingType,
                                                  "GET");
        return conn;
    }

    /**
     * Creates a basic put/get/delete request with common required headers.
     * 
     * @param urlString
     * @param key
     * @param storeName
     * @param originTime
     * @param timeOut
     * @param routingType
     * @param method
     * @return
     * @throws IOException
     */
    public HttpURLConnection createConnection(String urlString,
                                              String key,
                                              String storeName,
                                              String originTime,
                                              String timeOut,
                                              String routingType,
                                              String method) throws IOException {

        HttpURLConnection conn = null;
        URL url;
        String urlStr = urlString;
        String base64Key = "";
        if(storeName != null) {
            urlStr += storeName + "/";
        }
        if(key != null) {
            base64Key = new String(Base64.encodeBase64(key.getBytes()));
            url = new URL(urlStr + base64Key);
        } else {
            url = new URL(urlStr);
        }
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        conn.setDoInput(true);

        if(originTime != null) {
            conn.setRequestProperty(RestMessageHeaders.X_VOLD_REQUEST_ORIGIN_TIME_MS,
                                    String.valueOf(originTime));
        }
        if(timeOut != null) {
            conn.setRequestProperty(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS,
                                    String.valueOf(timeOut));
        }
        if(routingType != null) {
            conn.setRequestProperty(RestMessageHeaders.X_VOLD_ROUTING_TYPE_CODE,
                                    String.valueOf(routingType));
        }
        return conn;

    }

    public void readErrorMessageFromResponse(HttpURLConnection conn) throws IOException {
        BufferedReader bufferedReader;
        bufferedReader = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        while((line = bufferedReader.readLine()) != null) {
            stringBuilder.append(line);
        }
        bufferedReader.close();
        logger.info(stringBuilder.toString());
    }

    @Test
    public void testMissingRoutingType() {
        logger.info("********** Testing missing routing type **********");
        HttpURLConnection conn = null;
        int responseCode = -1;
        try {
            conn = doPut(urlStr,
                         key1,
                         value1,
                         storeNameStr,
                         String.valueOf(originTime),
                         String.valueOf(timeOut),
                         null,
                         eTag,
                         contentType,
                         String.valueOf(value1.getBytes().length));

            responseCode = conn.getResponseCode();
            logger.info("Response Code: " + responseCode + " Message: " + conn.getResponseMessage());
            readErrorMessageFromResponse(conn);
        } catch(IOException e1) {
            e1.printStackTrace();
        }
        assertEquals(responseCode, 400);
        conn.disconnect();
    }

    @Test
    public void testInvalidRoutingType() {
        logger.info("********** Testing invalid routing type **********");
        HttpURLConnection conn = null;
        int responseCode = -1;
        try {
            conn = doGet(urlStr,
                         key1,
                         storeNameStr,
                         String.valueOf(originTime),
                         String.valueOf(timeOut),
                         String.valueOf(6));

            responseCode = conn.getResponseCode();
            logger.info("Response Code: " + responseCode + " Message: " + conn.getResponseMessage());
            readErrorMessageFromResponse(conn);
        } catch(IOException e1) {
            e1.printStackTrace();
        }
        assertEquals(responseCode, 400);
        conn.disconnect();
    }

    @Test
    public void testNonNumberRoutingType() {
        logger.info("********** Testing non number Routing Type **********");
        HttpURLConnection conn = null;
        int responseCode = -1;
        try {
            conn = doDelete(urlStr,
                            key1,
                            storeNameStr,
                            String.valueOf(originTime),
                            String.valueOf(timeOut),
                            "asdfa",
                            eTag);

            responseCode = conn.getResponseCode();
            logger.info("Response Code: " + responseCode + " Message: " + conn.getResponseMessage());
            readErrorMessageFromResponse(conn);
        } catch(IOException e1) {
            e1.printStackTrace();
        }
        assertEquals(responseCode, 400);
        conn.disconnect();
    }

    @Test
    public void testMissingTimeOut() {
        logger.info("********** Testing missing time out **********");
        HttpURLConnection conn = null;
        int responseCode = -1;
        try {
            conn = doPut(urlStr,
                         key1,
                         value1,
                         storeNameStr,
                         String.valueOf(originTime),
                         null,
                         String.valueOf(routingType),
                         eTag,
                         contentType,
                         String.valueOf(value1.getBytes().length));
            responseCode = conn.getResponseCode();
            logger.info("Response Code: " + responseCode + " Message: " + conn.getResponseMessage());
            readErrorMessageFromResponse(conn);
        } catch(IOException e) {
            e.printStackTrace();
        }
        assertEquals(responseCode, 400);
        conn.disconnect();
    }

    @Test
    public void testNonNumberTimeOut() {
        logger.info("********** Testing non number time out **********");
        HttpURLConnection conn = null;
        int responseCode = -1;
        try {
            conn = doGet(urlStr,
                         key1,
                         storeNameStr,
                         String.valueOf(originTime),
                         "asdfasdf",
                         String.valueOf(routingType));
            responseCode = conn.getResponseCode();
            logger.info("Response Code: " + responseCode + " Message: " + conn.getResponseMessage());
            readErrorMessageFromResponse(conn);
        } catch(IOException e) {
            e.printStackTrace();
        }
        assertEquals(responseCode, 400);
        conn.disconnect();
    }

    @Test
    public void testMissingTimeStamp() {
        logger.info("********** Testing missing time stamp **********");
        HttpURLConnection conn = null;
        int responseCode = -1;
        try {
            conn = doDelete(urlStr,
                            key1,
                            storeNameStr,
                            null,
                            String.valueOf(timeOut),
                            String.valueOf(routingType),
                            eTag);
            responseCode = conn.getResponseCode();
            logger.info("Response Code: " + responseCode + " Message: " + conn.getResponseMessage());
            readErrorMessageFromResponse(conn);
        } catch(IOException e) {
            e.printStackTrace();
        }
        assertEquals(responseCode, 400);
        conn.disconnect();
    }

    @Test
    public void testNonNumberTimeStamp() {
        logger.info("********** Testing non number time out **********");
        HttpURLConnection conn = null;
        int responseCode = -1;
        try {
            conn = doPut(urlStr,
                         key1,
                         value1,
                         storeNameStr,
                         "asdfasdf",
                         String.valueOf(timeOut),
                         String.valueOf(routingType),
                         eTag,
                         contentType,
                         String.valueOf(value1.getBytes().length));
            responseCode = conn.getResponseCode();
            logger.info("Response Code: " + responseCode + " Message: " + conn.getResponseMessage());
            readErrorMessageFromResponse(conn);
        } catch(IOException e) {
            e.printStackTrace();
        }
        assertEquals(responseCode, 400);
        conn.disconnect();
    }

    @Test
    public void testMissingKey() {
        logger.info("********** Testing missing key **********");
        HttpURLConnection conn = null;
        int responseCode = -1;
        try {
            conn = doGet(urlStr,
                         null,
                         storeNameStr,
                         String.valueOf(originTime),
                         String.valueOf(timeOut),
                         String.valueOf(routingType));
            responseCode = conn.getResponseCode();
            logger.info("Response Code: " + responseCode + " Message: " + conn.getResponseMessage());
            readErrorMessageFromResponse(conn);
        } catch(IOException e) {
            e.printStackTrace();
        }
        assertEquals(responseCode, 400);
        conn.disconnect();
    }

    @Test
    public void testMissingStoreName() {
        logger.info("********** Testing missing store name **********");
        HttpURLConnection conn = null;
        int responseCode = -1;
        try {
            conn = doDelete(urlStr,
                            key1,
                            null,
                            String.valueOf(originTime),
                            String.valueOf(timeOut),
                            String.valueOf(routingType),
                            eTag);
            responseCode = conn.getResponseCode();
            logger.info("Response Code: " + responseCode + " Message: " + conn.getResponseMessage());
            readErrorMessageFromResponse(conn);
        } catch(IOException e) {
            e.printStackTrace();
        }
        assertEquals(responseCode, 400);
        conn.disconnect();
    }

    @Test
    public void testInvalidStoreName() {
        logger.info("********** Testing invalid store name **********");
        HttpURLConnection conn = null;
        int responseCode = -1;
        try {
            conn = doGet(urlStr,
                         key1,
                         "blahblah",
                         String.valueOf(originTime),
                         String.valueOf(timeOut),
                         String.valueOf(routingType));
            responseCode = conn.getResponseCode();
            logger.info("Response Code: " + responseCode + " Message: " + conn.getResponseMessage());
            readErrorMessageFromResponse(conn);
        } catch(IOException e) {
            e.printStackTrace();
        }
        assertEquals(responseCode, 400);
        conn.disconnect();
    }

    @Test
    public void testMissingVectorClock() {
        logger.info("********** Testing missing vector clock **********");
        HttpURLConnection conn = null;
        int responseCode = -1;
        try {
            conn = doPut(urlStr,
                         key1,
                         value1,
                         storeNameStr,
                         String.valueOf(originTime),
                         String.valueOf(timeOut),
                         String.valueOf(routingType),
                         null,
                         contentType,
                         String.valueOf(value1.getBytes().length));
            responseCode = conn.getResponseCode();
            logger.info("Response Code: " + responseCode + " Message: " + conn.getResponseMessage());
            readErrorMessageFromResponse(conn);
        } catch(IOException e) {
            e.printStackTrace();
        }
        assertEquals(responseCode, 400);
        conn.disconnect();
    }

    @Test
    public void testInvalidVectorClock() {
        logger.info("********** Testing invalid  vector clock **********");
        HttpURLConnection conn = null;
        int responseCode = -1;
        try {
            conn = doDelete(urlStr,
                            key1,
                            storeNameStr,
                            String.valueOf(originTime),
                            String.valueOf(timeOut),
                            String.valueOf(routingType),
                            "gvkjhgvlj");
            responseCode = conn.getResponseCode();
            logger.info("Response Code: " + responseCode + " Message: " + conn.getResponseMessage());
            readErrorMessageFromResponse(conn);
        } catch(IOException e) {
            e.printStackTrace();
        }
        assertEquals(responseCode, 400);
        conn.disconnect();
    }

    @Test
    public void testGetWithNonExistingKey() {
        logger.info("********** Testing get with non existing key **********");
        HttpURLConnection conn = null;
        int responseCode = -1;
        try {
            conn = doGet(urlStr,
                         "non existing key",
                         storeNameStr,
                         String.valueOf(originTime),
                         String.valueOf(timeOut),
                         String.valueOf(routingType));
            responseCode = conn.getResponseCode();
            logger.info("Response Code: " + responseCode + " Message: " + conn.getResponseMessage());
            readErrorMessageFromResponse(conn);
        } catch(IOException e) {
            e.printStackTrace();
        }
        assertEquals(responseCode, 404);
        conn.disconnect();
    }

    @Test
    public void testGetAllWithNonExistingKey() {
        logger.info("********** Testing get all with all non existing keys **********");
        HttpURLConnection conn = null;
        int responseCode = -1;
        try {
            conn = doGet(urlStr,
                         "non existing key1, non existing key2",
                         storeNameStr,
                         String.valueOf(originTime),
                         String.valueOf(timeOut),
                         String.valueOf(routingType));
            responseCode = conn.getResponseCode();
            logger.info("Response Code: " + responseCode + " Message: " + conn.getResponseMessage());
            readErrorMessageFromResponse(conn);
        } catch(IOException e) {
            e.printStackTrace();
        }
        assertEquals(responseCode, 404);
        conn.disconnect();
    }

    @Test
    public void testObsoleteVersionException() throws IOException {
        logger.info("********** Testing obsolete version exception **********");

        // setUP
        deleteKeysCreated(key1, eTag);

        HttpURLConnection conn = null;
        int responseCode = -1;
        try {
            conn = doPut(urlStr,
                         key1,
                         value1,
                         storeNameStr,
                         String.valueOf(originTime),
                         String.valueOf(timeOut),
                         String.valueOf(routingType),
                         eTag,
                         contentType,
                         String.valueOf(value1.getBytes().length));
            responseCode = conn.getResponseCode();
            logger.info("Response Code: " + responseCode + " Message: " + conn.getResponseMessage());
            if(responseCode != 200 && responseCode != 201) {
                fail("Initial put failed");
            } else {
                conn.disconnect();
                conn = doPut(urlStr,
                             key1,
                             value1,
                             storeNameStr,
                             String.valueOf(originTime),
                             String.valueOf(timeOut),
                             String.valueOf(routingType),
                             eTag,
                             contentType,
                             String.valueOf(value1.getBytes().length));
                responseCode = conn.getResponseCode();
                logger.info("Response Code: " + responseCode + " Message: "
                            + conn.getResponseMessage());
                readErrorMessageFromResponse(conn);
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
        assertEquals(responseCode, 412);
        conn.disconnect();

        // cleanup
        deleteKeysCreated(key1, eTag);
    }

    @Test
    public void testDeleteWithNonExistingKey() {
        logger.info("********** Testing delete non existing key **********");
        HttpURLConnection conn = null;
        int responseCode = -1;
        try {
            conn = doDelete(urlStr,
                            "non existing key",
                            storeNameStr,
                            String.valueOf(originTime),
                            String.valueOf(timeOut),
                            String.valueOf(routingType),
                            eTag);
            responseCode = conn.getResponseCode();
            logger.info("Response Code: " + responseCode + " Message: " + conn.getResponseMessage());
            readErrorMessageFromResponse(conn);

        } catch(IOException e) {
            e.printStackTrace();
        }
        assertEquals(responseCode, 404);
        conn.disconnect();
    }

    @Test
    public void testDeleteLowerVersion() throws IOException {
        logger.info("********** Testing delete lower version **********");
        // setup
        deleteKeysCreated(key1, eTag);

        HttpURLConnection conn = null;
        int responseCode = -1;
        String eTag2 = null;
        try {
            conn = doPut(urlStr,
                         key1,
                         value1,
                         storeNameStr,
                         String.valueOf(originTime),
                         String.valueOf(timeOut),
                         String.valueOf(routingType),
                         eTag,
                         contentType,
                         String.valueOf(value1.getBytes().length));
            responseCode = conn.getResponseCode();
            logger.info("Response Code: " + responseCode + " Message: " + conn.getResponseMessage());
            conn.disconnect();
            if(responseCode != 200 && responseCode != 201) {
                fail("Initial put failed");
            } else {
                VectorClock vc = vectorClock.incremented(config.getNodeId(),
                                                         System.currentTimeMillis());
                eTag2 = CoordinatorUtils.getSerializedVectorClock(vc);
                String value2 = "The next longest value";
                conn = doPut(urlStr,
                             key1,
                             value2,
                             storeNameStr,
                             String.valueOf(originTime),
                             String.valueOf(timeOut),
                             String.valueOf(routingType),
                             eTag2,
                             contentType,
                             String.valueOf(value2.getBytes().length));
                responseCode = conn.getResponseCode();
                logger.info("Response Code: " + responseCode + " Message: "
                            + conn.getResponseMessage());
                conn.disconnect();
                if(responseCode != 201) {
                    fail("Second put failed");
                } else {
                    conn = doDelete(urlStr,
                                    key1,
                                    storeNameStr,
                                    String.valueOf(originTime),
                                    String.valueOf(timeOut),
                                    String.valueOf(routingType),
                                    eTag);
                    responseCode = conn.getResponseCode();
                    logger.info("Response Code: " + responseCode + " Message: "
                                + conn.getResponseMessage());
                    readErrorMessageFromResponse(conn);
                }
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
        assertEquals(responseCode, 404);
        conn.disconnect();

        // cleanUP specific to this test case
        deleteKeysCreated(key1, eTag);
        deleteKeysCreated(key1, eTag2);

    }

    // cleanup method
    public void deleteKeysCreated(String key, String eTag) throws IOException {
        HttpURLConnection conn = null;
        conn = doDelete(urlStr,
                        key,
                        storeNameStr,
                        String.valueOf(originTime),
                        String.valueOf(timeOut),
                        String.valueOf(routingType),
                        eTag);
        conn.getResponseCode();
        conn.disconnect();
    }
}
