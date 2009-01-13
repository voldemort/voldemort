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

package voldemort.store.memory;

import java.util.ArrayList;
import java.util.List;

import voldemort.TestUtils;
import voldemort.store.StorageEngine;
import voldemort.store.StorageEngineTest;

public class InMemoryStorageEngineTest extends StorageEngineTest {

    private InMemoryStorageEngine<byte[], byte[]> store;

    @Override
    public StorageEngine<byte[], byte[]> getStorageEngine() {
        return store;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.store = new InMemoryStorageEngine<byte[], byte[]>("test");
    }

    @Override
    public List<byte[]> getKeys(int numKeys) {
        List<byte[]> keys = new ArrayList<byte[]>(numKeys);
        for(int i = 0; i < numKeys; i++)
            keys.add(TestUtils.randomBytes(10));
        return keys;
    }

}
