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

package voldemort.performance;

import java.io.File;
import java.io.FileReader;
import java.util.Map;

import voldemort.client.DefaultStoreClient;
import voldemort.client.StoreClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.StringSerializer;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.Store;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.routed.RoutedStore;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.versioned.InconsistencyResolvingStore;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.VectorClockInconsistencyResolver;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

import com.google.common.collect.Maps;

public class LocalRoutedStoreLoadTest extends AbstractLoadTestHarness {

    @Override
    public StoreClient<String, String> getStore(Props propsA, Props propsB) throws Exception {
        Cluster cluster = new ClusterMapper().readCluster(new FileReader(propsA.getString("metadata.directory")
                                                                         + File.separator
                                                                         + "/cluster.xml"));
        RoutingStrategy routingStrategy = new ConsistentRoutingStrategy(cluster.getNodes(), 1);
        Map<Integer, Store<ByteArray, byte[]>> clientMapping = Maps.newHashMap();
        StorageConfiguration conf = new BdbStorageConfiguration(new VoldemortConfig(propsA));
        for(Node node: cluster.getNodes())
            clientMapping.put(node.getId(), conf.getStore("test" + node.getId()));

        InconsistencyResolver<Versioned<String>> resolver = new VectorClockInconsistencyResolver<String>();
        Store<ByteArray, byte[]> store = new RoutedStore("test",
                                                         clientMapping,
                                                         routingStrategy,
                                                         1,
                                                         1,
                                                         10,
                                                         true,
                                                         10000L);
        Store<String, String> serializingStore = new SerializingStore<String, String>(store,
                                                                                      new StringSerializer(),
                                                                                      new StringSerializer());
        return new DefaultStoreClient<String, String>(new InconsistencyResolvingStore<String, String>(serializingStore,
                                                                                                      resolver),
                                                      new StringSerializer(),
                                                      new StringSerializer(),
                                                      null);
    }

    public static void main(String[] args) throws Exception {
        new LocalRoutedStoreLoadTest().run(args);
    }

}
