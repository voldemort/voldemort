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
