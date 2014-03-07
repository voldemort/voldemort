/*
 * Copyright 2008-2010 LinkedIn, Inc
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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import voldemort.serialization.StringSerializer;
import voldemort.versioning.TimeBasedInconsistencyResolver;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class CachingStoreClientFactoryTest {

    private final boolean useLazy;

    public CachingStoreClientFactoryTest(boolean useLazy) {
        this.useLazy = useLazy;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Test
    public void testCaching() {
        StoreClientFactory inner = new MockStoreClientFactory(new StringSerializer(),
                                                              new StringSerializer(),
                                                              null);
        StoreClientFactory spyFactory = spy(inner);
        StoreClientFactory cachingFactory = new CachingStoreClientFactory(spyFactory);
        TimeBasedInconsistencyResolver<Object> resolver = new TimeBasedInconsistencyResolver<Object>();

        when(spyFactory.<Object, Object>getStoreClient(anyString())).thenCallRealMethod();
        when(spyFactory.<Object, Object>getStoreClient(anyString(), eq(resolver))).thenCallRealMethod();

        for(int i = 0; i < 10; i++) {
            assertNotNull(cachingFactory.getStoreClient("foo"));
            assertNotNull(cachingFactory.getStoreClient("foo", resolver));
        }

        verify(spyFactory, times(1)).getStoreClient("foo", resolver);
        verify(spyFactory, times(1)).getStoreClient("foo", null);
    }

    @Test
    public void testBootstrapAll() {
        StoreClientFactory inner = new MockStoreClientFactory(new StringSerializer(),
                                                              new StringSerializer(),
                                                              null);
        final DefaultStoreClient<Object, Object> aStoreClient = spy((DefaultStoreClient<Object, Object>) inner.getStoreClient("test1"));
        final DefaultStoreClient<Object, Object> bStoreClient = spy((DefaultStoreClient<Object, Object>) inner.getStoreClient("test2"));
        StoreClientFactory mocked = mock(StoreClientFactory.class);

        if(useLazy) {
            when(mocked.<Object, Object>getStoreClient("test1", null)).thenReturn(createLazyStoreClient(aStoreClient));
            when(mocked.<Object, Object>getStoreClient("test2", null)).thenReturn(createLazyStoreClient(bStoreClient));
        } else {
            when(mocked.<Object, Object>getStoreClient("test1", null)).thenReturn(aStoreClient);
            when(mocked.<Object, Object>getStoreClient("test2", null)).thenReturn(bStoreClient);
        }

        CachingStoreClientFactory cachingFactory = new CachingStoreClientFactory(mocked);
        cachingFactory.getStoreClient("test1");
        cachingFactory.getStoreClient("test2");
        cachingFactory.bootstrapAllClients();

        verify(aStoreClient, times(1)).bootStrap();
        verify(bStoreClient, times(1)).bootStrap();
    }

    private LazyStoreClient<Object, Object> createLazyStoreClient(final DefaultStoreClient<Object, Object> storeClient                                                                 ) {
        return new LazyStoreClient<Object, Object>(new Callable<StoreClient<Object, Object>>() {
            public StoreClient<Object, Object> call() throws Exception {
                return storeClient;
            }
        });
    }
}
