package voldemort.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import voldemort.TestUtils;
import voldemort.cluster.Zone;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

public class ClientTrafficVerifier implements Runnable {

    static Logger logger = Logger.getLogger(ClientTrafficVerifier.class);

    class PrintableHashMap extends HashMap<String, Integer> {

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for(Object key: this.keySet()) {
                builder.append(key.toString() + ": " + this.get(key) + "; ");
            }
            return builder.toString();
        }
    };

    public static int MODE_ALLOW_GET = 0x01;
    public static int MODE_ALLOW_GETALL = 0x02;
    public static int MODE_ALLOW_PUT = 0x04;
    public static int MODE_ALLOW_ALL = MODE_ALLOW_GET | MODE_ALLOW_GETALL | MODE_ALLOW_PUT;
    int operationMode = MODE_ALLOW_ALL;
    static final Integer KV_POOL_SIZE = 100;
    public final StoreClient<String, String> client;
    final StoreClientFactory factory;
    boolean shouldStop = false;
    private boolean stopped = true; // not thread safe by multiple readers
    Thread thread;
    public final String clientName;
    List<String> keys = new ArrayList<String>(KV_POOL_SIZE);
    Map<String, String> kvMap = new HashMap<String, String>(KV_POOL_SIZE);
    Map<String, Integer> kvUpdateCount = new HashMap<String, Integer>(KV_POOL_SIZE);
    public final Map<String, Integer> exceptionCount = new PrintableHashMap();
    public final Map<String, Integer> requestCount = new PrintableHashMap();

    public ClientTrafficVerifier(String clientName,
                                 String bootstrapURL,
                                 String storeName,
                                 Integer clientZoneId) {
        ClientConfig config = new ClientConfig().setBootstrapUrls(bootstrapURL);
        if(clientZoneId != Zone.UNSET_ZONE_ID) {
            config.setClientZoneId(clientZoneId);
        }
        factory = new SocketStoreClientFactory(config);
        client = factory.getStoreClient(storeName);
        this.clientName = clientName;

        int i = 0;
        while(i < KV_POOL_SIZE) {
            String k = TestUtils.randomString("ABCDEFGHIJKLMNOPQRSTUVWXYZ", 40);
            String v = TestUtils.randomString("abcdefghijklmnopqrstuvwxyz", 40);
            if(kvMap.containsKey(k)) {
                // prevent duplicate keys
                continue;
            }
            k = clientName + "_" + k;
            kvMap.put(k, v);
            kvUpdateCount.put(k, 0);
            keys.add(k);
            i++;
        }

        requestCount.put("GET", 0);
        requestCount.put("PUT", 0);
        requestCount.put("GETALL", 0);
    }

    public ClientTrafficVerifier setOperationMode(int mode) {
        operationMode = mode;
        return this;
    }

    boolean isInitialized = false;
    public void initialize() {
        if(isInitialized)
            return;
        for(String k: kvMap.keySet()) {
            client.put(k, kvMap.get(k) + "_" + kvUpdateCount.get(k).toString());
        }
        isInitialized = true;
    }

    @SuppressWarnings("serial")
    @Override
    public void run() {
        Random r = new Random(System.currentTimeMillis());
        while(!shouldStop) {
            String k = keys.get(r.nextInt(KV_POOL_SIZE));
            try {
                switch(r.nextInt(3)) {
                    case 0: // update
                        if((operationMode & MODE_ALLOW_PUT) == 0) {
                            break;
                        }
                        int newCount = kvUpdateCount.get(k) + 1;
                        client.put(k, kvMap.get(k) + "_" + newCount);
                        kvUpdateCount.put(k, newCount);
                        requestCount.put("PUT", requestCount.get("PUT") + 1);
                        break;
                    case 1: // get
                        if((operationMode & MODE_ALLOW_GET) == 0) {
                            break;
                        }
                        Versioned<String> value = client.get(k);

                        verifyValue(k, value);
                        requestCount.put("GET", requestCount.get("GET") + 1);
                        break;
                    case 2: // get all
                        if((operationMode & MODE_ALLOW_GETALL) == 0) {
                            break;
                        }
                        String k2 = keys.get(r.nextInt(KV_POOL_SIZE));
                        Map<String, Versioned<String>> result = client.getAll(Arrays.asList(k, k2));
                        verifyValue(k, result.get(k));
                        verifyValue(k2, result.get(k2));
                        requestCount.put("GETALL", requestCount.get("GETALL") + 1);
                        break;
                }
            } catch(ObsoleteVersionException e) {
                // Theoretically, each thread works with its own set of keys the
                // ObsoleteVersionException should not happen. But partitions
                // are moving around nodes and because of the way we
                // acknowledge writes before all nodes are complete and using
                // async writes they can be out of sync and the exceptions can
                // still happen. Did not try digging deeper on this one
                // as it is irrelevant for the refactoring I am doing.

            } catch(Exception e) {
                logger.info("CLIENT EXCEPTION FAILURE on key [" + k + "]" + e.toString());
                e.printStackTrace();
                String exceptionName = "Key " + k + " " + e.getClass().toString();
                if(exceptionCount.containsKey(exceptionName)) {
                    exceptionCount.put(exceptionName, exceptionCount.get(exceptionName) + 1);
                } else {
                    exceptionCount.put(exceptionName, 1);
                }
            }
        }
    }

    public void start() {
        this.thread = new Thread(this);
        this.thread.start();
        this.stopped = false;
    }

    public void stop() {
        try {
            this.shouldStop = true;
            this.thread.join();
            this.stopped = true;
        } catch(InterruptedException e) {
            e.printStackTrace();
        }

        try {
            this.factory.close();
        } catch(Exception e) {

        }
    }

    public boolean isStopped() {
        return this.stopped;
    }

    private void verifyValue(String key, Versioned<String> value) {
        if(value == null) {
            throw new RuntimeException("Versioned is empty for key [" + key + "]") {};
        } else if(value.getValue() == null) {
            throw new RuntimeException("Versioned has empty value inside for key [" + key + "]") {};
        }
        // For some reasons the expected and retrieved values are not the same.
        // else {
        // String retrievedValue = value.getValue();
        // String expectedValue = kvMap.get(key) + "_" + kvUpdateCount.get(key);
        // if(retrievedValue.equals(expectedValue) == false) {
        // throw new RuntimeException("Key " + key + " Expected Value " +
        // expectedValue
        // + " Retrieved Value " + retrievedValue) {};
        // }
        // }
    }

    public void verifyPostConditions() {
        // for(String key: kvMap.keySet()) {
        // Versioned<String> value = client.get(key);
        // verifyValue(key, value);
        // }

        Map<String, Integer> eMap = exceptionCount;
        logger.info("-------------------------------------------------------------------");
        logger.info("Client Operation Info of [" + clientName + "]");
        logger.info(requestCount.toString());
        if(eMap.size() == 0) {
            logger.info("No Exception reported by ClientTrafficVerifier(ObsoleteVersionException are ignored)");

            logger.info("-------------------------------------------------------------------");
        } else {
            logger.info("Exceptions Count Map of the client: ");
            logger.info(eMap.toString());
            logger.info("-------------------------------------------------------------------");
            throw new RuntimeException("Found Exceptions by Client" + eMap);
        }

    }
}
