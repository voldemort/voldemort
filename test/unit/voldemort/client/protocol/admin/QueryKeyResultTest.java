/*
 * Copyright 2013 LinkedIn, Inc
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
package voldemort.client.protocol.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import voldemort.TestUtils;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class QueryKeyResultTest {

    @Test
    public void testStandardCtor() {
        ByteArray key = new ByteArray("key".getBytes());
        List<Versioned<byte[]>> values = new ArrayList<Versioned<byte[]>>(0);

        Versioned<byte[]> value1 = TestUtils.getVersioned(TestUtils.randomBytes(10), 1, 1, 1);
        values.add(value1);

        Versioned<byte[]> value2 = TestUtils.getVersioned(TestUtils.randomBytes(10), 1, 1, 2);
        values.add(value2);

        QueryKeyResult queryKeyResult = new QueryKeyResult(key, values);

        assertTrue(queryKeyResult.hasValues());
        assertEquals(values, queryKeyResult.getValues());

        assertFalse(queryKeyResult.hasException());
        assertEquals(null, queryKeyResult.getException());
    }

    @Test
    public void testExceptionCtor() {
        ByteArray key = new ByteArray("key".getBytes());

        Exception e = new Exception();
        QueryKeyResult queryKeyResult = new QueryKeyResult(key, e);

        assertFalse(queryKeyResult.hasValues());
        assertEquals(null, queryKeyResult.getValues());

        assertTrue(queryKeyResult.hasException());
        assertEquals(e, queryKeyResult.getException());
    }

}
