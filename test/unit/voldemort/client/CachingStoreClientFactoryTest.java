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
import voldemort.serialization.StringSerializer;
import voldemort.versioning.TimeBasedInconsistencyResolver;

import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class CachingStoreClientFactoryTest {

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

        verify(spyFactory, times(1)).getStoreClient("foo");
        verify(spyFactory, times(2)).getStoreClient("foo", resolver);
    }
}
