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

import voldemort.StaticStoreClientFactory;
import voldemort.client.DefaultStoreClient;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.serialization.StringSerializer;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.Store;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.versioned.InconsistencyResolvingStore;
import voldemort.utils.Props;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.VectorClockInconsistencyResolver;
import voldemort.versioning.Versioned;

public class LocalDirectLoadTest extends AbstractLoadTestHarness {

    @Override
    public StoreClient<String, String> getStore(Props propsA, Props propsB) throws Exception {
        StorageConfiguration conf = new BdbStorageConfiguration(new VoldemortConfig(propsA));
        Store<String, String, String> store = SerializingStore.wrap(conf.getStore("test" + 0),
                                                                    new StringSerializer(),
                                                                    new StringSerializer(),
                                                                    new StringSerializer());
        InconsistencyResolver<Versioned<String>> resolver = new VectorClockInconsistencyResolver<String>();
        Store<String, String, String> resolvingStore = new InconsistencyResolvingStore<String, String, String>(store,
                                                                                                               resolver);
        StoreClientFactory factory = new StaticStoreClientFactory(resolvingStore);
        return new DefaultStoreClient<String, String>(store.getName(), resolver, factory, 1);
    }

    public static void main(String[] args) throws Exception {
        new LocalDirectLoadTest().run(args);
    }

}
