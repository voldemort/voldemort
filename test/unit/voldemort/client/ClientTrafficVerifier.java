package voldemort.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import voldemort.TestUtils;
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
    public boolean stopped = true; // not thread safe by multiple readers
    final Thread thread;
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
        factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapURL)
                                                                 .setClientZoneId(clientZoneId));
        client = factory.getStoreClient(storeName);
        thread = new Thread(this);
        this.clientName = clientName;

        int i = 0;
        while(i < KV_POOL_SIZE) {
            String k = TestUtils.randomString("ABCDEFGHIJKLMNOPQRSTUVWXYZ", 40);
            String v = TestUtils.randomString("abcdefghijklmnopqrstuvwxyz", 40);
            if(kvMap.containsKey(k)) {
                // prevent duplicate keys
                continue;
            }
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

    public void initialize() {
        for(String k: kvMap.keySet()) {
            client.put(k, kvMap.get(k) + "_" + kvUpdateCount.get(k).toString());
        }
    }

    @SuppressWarnings("serial")
    @Override
    public void run() {
        Random r = new Random(System.currentTimeMillis());
        while(!shouldStop) {
            String k = keys.get(r.nextInt(KV_POOL_SIZE));
            try {
                switch(r.nextInt(3)){
                    case 0 : // update
                        if((operationMode & MODE_ALLOW_PUT) == 0) {
                            break;
                        }
                        kvUpdateCount.put(k, kvUpdateCount.get(k) + 1);
                        client.put(k, kvMap.get(k) + "_" + kvUpdateCount.get(k).toString());
                        requestCount.put("PUT", requestCount.get("PUT") + 1);
                        break;
                    case 1 : // get
                        if((operationMode & MODE_ALLOW_GET) == 0) {
                            break;
                        }
                        Versioned<String> value = client.get(k); // does not check versions, just prevent exceptions
                        if(value == null) {
                            throw new RuntimeException("Versioned is empty for key [" + k + "]") {};
                        } else {
                            if(value.getValue() == null) {
                                throw new RuntimeException("Versioned has empty value inside for key ["
                                                           + k + "]") {};
                            }
                        }
                        requestCount.put("GET", requestCount.get("GET") + 1);
                        break;
                    case 2 : // get all
                        if((operationMode & MODE_ALLOW_GETALL) == 0) {
                            break;
                        }
                        String k2 = keys.get(r.nextInt(KV_POOL_SIZE));
                        Map<String, Versioned<String>> result = client.getAll(Arrays.asList(k, k2));
                        if(result.get(k) == null) {
                            throw new RuntimeException("Versioned is empty for key [" + k + "]") {};
                        } else {
                            if(result.get(k).getValue() == null) {
                                throw new RuntimeException("Versioned has empty value inside for key ["
                                                           + k + "]") {};
                            }
                        }
                        if(result.get(k2) == null) {
                            throw new RuntimeException("Versioned is empty for key [" + k2 + "]") {};
                        } else {
                            if(result.get(k2).getValue() == null) {
                                throw new RuntimeException("Versioned has empty value inside for key ["
                                                           + k2 + "]") {};
                            }
                        }
                        requestCount.put("GETALL", requestCount.get("GETALL") + 1);
                        break;
                }
            } catch(ObsoleteVersionException e) {} catch(Exception e) {
                logger.info("CLIENT EXCEPTION FAILURE on key [" + k + "]" + e.toString());
                e.printStackTrace();
                String exceptionName = e.getClass().toString();
                if(exceptionCount.containsKey(exceptionName)) {
                    exceptionCount.put(exceptionName, exceptionCount.get(exceptionName) + 1);
                } else {
                    exceptionCount.put(exceptionName, 1);
                }
            }
        }
    }

    public void start() {
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
    }
}
