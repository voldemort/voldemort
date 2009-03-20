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
import java.util.Random;

public class MongoDBClient {

    protected final StoreClientFactory _factory;
    Random _rand = new Random();


    public MongoDBClient(String bootstrapURL) {
        // In real life this stuff would get wired in
        int numThreads = 10;
        int maxQueuedRequests = 10;
        int maxConnectionsPerNode = 10;
        int maxTotalConnections = 100;

        _factory = new SocketStoreClientFactory(
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
                                bootstrapURL);
    }


    public long multiWriteLarge(int count, String keyRoot) {

        StoreClient<String , Doc> client = _factory.getStoreClient("test");

        long start = System.currentTimeMillis();

        for (int i=0; i < count; i++) {
            Doc d = makeLargeDoc();
            d.add("x", 1);

            Versioned<Doc> v  = new Versioned<Doc>(d);

            client.put(keyRoot + i, v);
        }

        long end = System.currentTimeMillis();

        return end - start;
    }

    public long multiWrite(int count, String keyRoot){

        StoreClient<String , Doc> client = _factory.getStoreClient("test");

        long start = System.currentTimeMillis();

        for (int i=0; i < count; i++) {
            Doc d = new Doc("name", "geir");
            d.add("x", 1);

            Versioned<Doc> v  = new Versioned<Doc>(d);

            client.put(keyRoot + i, v);
        }

        long end = System.currentTimeMillis();

        return end - start;
    }

    public long multiRead(int count, String keyRoot){

        StoreClient<String , Doc> client = _factory.getStoreClient("test");

        long start = System.currentTimeMillis();

        int found = 0;

        for (int i=0; i < count; i++) {
            Versioned<Doc> v  = client.get(keyRoot + i);

            if (v != null) {
                found++;
            }
        }

        long end = System.currentTimeMillis();

        System.out.println("Found : " + found);
        
        return end - start;
    }

    public String getRandomKey(int n) {

        if (n <= 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder();

        for (int i=0; i < n; i++) {
            sb.append(_rand.nextInt(9));
        }

        return sb.toString();
    }

    public void simple(){

        StoreClient<String , Doc> client = _factory.getStoreClient("test");

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

    public Doc makeLargeDoc() {

        Doc d = new Doc();

        d.add("pb_id", 2321232);
        d.add("base_url", "http://www.example.com/test-me");
        d.add("total_word_count", 6743);
        d.add("access_time", 1234915320);

        Doc mt = new Doc();

        mt.add("description", "i am a long description string");
        mt.add("author", "Holly man");
        mt.add("dynamically_created_meta_tag", "who know what");

        d.add("meta_tags", mt);

        mt = new Doc();

        mt.add("counted_tags", 3450);
        mt.add("no_of_js_attached",10);
        mt.add("no_of_images", 6);

        d.put("page_structure", mt);

        mt = new Doc();
        for (int i = 0; i < 10; i ++) {
            mt.add(Integer.toString(i), "woog");
        }

        d.add("harvested_words", mt);

        return d;
    }

    public static void main(String[] args) {

        MongoDBClient client = new MongoDBClient("tcp://localhost:6666");

        client.simple();

        String keyRoot = client.getRandomKey(15);
        
        System.out.println(10000.0 / client.multiWrite(10000, keyRoot) * 1000 + " writes per sec");
        System.out.println(10000.0 / client.multiRead(10000, keyRoot) * 1000 + " reads per sec");

        keyRoot = client.getRandomKey(15);

        System.out.println(10000.0 / client.multiWriteLarge(10000, keyRoot) * 1000 + " large writes per sec");
    }

}
