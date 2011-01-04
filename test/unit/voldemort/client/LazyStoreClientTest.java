/*
 * Copyright 2008-2011 LinkedIn, Inc
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

package voldemort.client;

import org.junit.Before;
import org.junit.Test;
import voldemort.serialization.Serializer;
import voldemort.serialization.StringSerializer;
import voldemort.utils.SystemTime;

import java.util.concurrent.Callable;

import static org.mockito.Mockito.*;

/**
 */
public class LazyStoreClientTest extends DefaultStoreClientTest {

    private MockStoreClientFactory factory;

    @Override
    @Before
    public void setUp() {
        this.nodeId = 0;
        this.time = SystemTime.INSTANCE;
        Serializer<String> serializer = new StringSerializer();
        this.factory = new MockStoreClientFactory(serializer,
                                                  serializer,
                                                  null,
                                                  serializer,
                                                  nodeId,
                                                  time);
        this.client = newLazyStoreClient(factory);
    }

    @Test
    public void testInitializationShouldBeLazy() {
        StoreClientFactory spyFactory = spy(factory);
        LazyStoreClient<String, String> spyLazyClient = spy(newLazyStoreClient(spyFactory));

        // Check that we don't initialize upon construction
        verify(spyFactory, times(0)).getStoreClient("test");

        // Check that we initialize once and only once
        for(int i = 0; i < 10; i++)
            spyLazyClient.get("test");

        verify(spyFactory, times(1)).getStoreClient("test");
        verify(spyLazyClient, times(1)).initStoreClient();
    }

    private LazyStoreClient<String, String> newLazyStoreClient(final StoreClientFactory factory) {
        return new LazyStoreClient<String, String>(new Callable<StoreClient<String, String>>() {

            public StoreClient<String, String> call() throws Exception {
                return factory.getStoreClient("test");
            }
        });
    }
}
