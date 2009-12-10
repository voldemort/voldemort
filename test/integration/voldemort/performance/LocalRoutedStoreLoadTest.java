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

import voldemort.ServerTestUtils;
import voldemort.StaticStoreClientFactory;
import voldemort.client.DefaultStoreClient;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.BasicStoreResolver;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetectorUtils;
import voldemort.routing.RoutingStrategyType;
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
        Map<Integer, Store<ByteArray, byte[]>> clientMapping = Maps.newHashMap();
        VoldemortConfig voldemortConfig = new VoldemortConfig(propsA);
        StorageConfiguration conf = new BdbStorageConfiguration(voldemortConfig);
        for(Node node: cluster.getNodes())
            clientMapping.put(node.getId(), conf.getStore("test" + node.getId()));

        InconsistencyResolver<Versioned<String>> resolver = new VectorClockInconsistencyResolver<String>();

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig(voldemortConfig).setNodes(cluster.getNodes())
                                                                                                .setStoreResolver(new BasicStoreResolver(clientMapping));
        FailureDetector failureDetector = FailureDetectorUtils.create(failureDetectorConfig);

        Store<ByteArray, byte[]> store = new RoutedStore("test",
                                                         clientMapping,
                                                         cluster,
                                                         ServerTestUtils.getStoreDef("test",
                                                                                     1,
                                                                                     1,
                                                                                     1,
                                                                                     1,
                                                                                     1,
                                                                                     RoutingStrategyType.CONSISTENT_STRATEGY),
                                                         10,
                                                         true,
                                                         10000L,
                                                         failureDetector);
        /*
         * public DefaultStoreClient(String storeName,
         * InconsistencyResolver<Versioned<V>> resolver, StoreClientFactory
         * storeFactory, int maxMetadataRefreshAttempts) {
         */
        Store<String, String> serializingStore = SerializingStore.wrap(store,
                                                                       new StringSerializer(),
                                                                       new StringSerializer());
        Store<String, String> resolvingStore = new InconsistencyResolvingStore<String, String>(serializingStore,
                                                                                               resolver);
        StoreClientFactory factory = new StaticStoreClientFactory(resolvingStore);
        return new DefaultStoreClient<String, String>(store.getName(), resolver, factory, 1);
    }

    public static void main(String[] args) throws Exception {
        new LocalRoutedStoreLoadTest().run(args);
    }

}
