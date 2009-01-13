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

import static voldemort.VoldemortTestConstants.getOneNodeClusterXml;
import static voldemort.VoldemortTestConstants.getSimpleStoreDefinitionsXml;
import static voldemort.utils.Utils.rm;

import java.io.File;
import java.io.StringReader;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.httpclient.HttpClient;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.serialization.StringSerializer;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.http.HttpStore;
import voldemort.store.serialized.SerializingStore;
import voldemort.utils.Props;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

public class ClientServerTest extends TestCase {

    private VoldemortServer server;
    private Store<String, String> store;
    private String tempDir;

    public ClientServerTest(String message) {
        super(message);
    }

    public void setUp() {
        Props serverProps = new Props();
        this.tempDir = System.getProperty("java.io.tmpdir") + File.separator
                       + new Random().nextInt(Integer.MAX_VALUE);
        assertTrue(new File(tempDir).mkdir());
        String dataDir = tempDir + File.separator + "data";
        assertTrue(new File(dataDir).mkdir());
        String metaDir = tempDir + File.separator + "meta";
        assertTrue(new File(metaDir).mkdir());
        String storesDir = tempDir + File.separator + "stores";
        assertTrue(new File(storesDir).mkdir());

        serverProps.put("voldemort.home", tempDir);
        serverProps.put("bdb.data.directory", dataDir);
        serverProps.put("metadata.directory", metaDir);
        serverProps.put("data.directory", storesDir);
        serverProps.put("node.id", 0);
        serverProps.put("bdb.cache.size", "50MB");
        serverProps.put("max.threads", 1);

        Cluster cluster = new ClusterMapper().readCluster(new StringReader(getOneNodeClusterXml()));
        List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(getSimpleStoreDefinitionsXml()));

        server = new VoldemortServer(serverProps, cluster);
        server.start();

        String storeName = storeDefs.get(0).getName();
        Node node = server.getIdentityNode();
        Store<String, String> store = new SerializingStore<String, String>(new HttpStore(storeName,
                                                                                         node.getHost(),
                                                                                         node.getHttpPort(),
                                                                                         new HttpClient()),
                                                                           new StringSerializer(),
                                                                           new StringSerializer());
    }

    public void tearDown() {
        server.stop();
        rm(tempDir);
    }

    public void testBasicActions() {

    }

}
