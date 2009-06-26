package voldemort.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormat;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public abstract class AbstractRequestFormatTest extends TestCase {

    private final String storeName;
    private final RequestFormat clientWireFormat;
    private final RequestHandler serverWireFormat;
    private final InMemoryStorageEngine<ByteArray, byte[]> store;

    public AbstractRequestFormatTest(RequestFormatType type) {
        this.storeName = "test";
        this.store = new InMemoryStorageEngine<ByteArray, byte[]>(storeName);
        StoreRepository repository = new StoreRepository();
        repository.addLocalStore(store);
        repository.addRoutedStore(store);
        this.clientWireFormat = new RequestFormatFactory().getRequestFormat(type);
        this.serverWireFormat = new RequestHandlerFactory(repository,
                                                          null,
                                                          ServerTestUtils.getVoldemortConfig()).getRequestHandler(type);
    }

    public void testNullKeys() throws Exception {
        try {
            testGetRequest(null, null, null, false);
            fail("Null key allowed.");
        } catch(IllegalArgumentException e) {
            // this is good
        }
        try {
            testGetAllRequest(new ByteArray[] { null }, null, null, new boolean[] { false });
            fail("Null key allowed.");
        } catch(IllegalArgumentException e) {
            // this is good
        }
        try {
            testPutRequest(null, null, null, null);
            fail("Null key allowed.");
        } catch(IllegalArgumentException e) {
            // this is good
        }
        try {
            testDeleteRequest(null, null, null, false);
            fail("Null key allowed.");
        } catch(IllegalArgumentException e) {
            // this is good
        }
    }

    public void testGetRequests() throws Exception {
        testGetRequest(TestUtils.toByteArray("hello"), null, null, false);
        testGetRequest(TestUtils.toByteArray("hello"), "".getBytes(), new VectorClock(), true);
        testGetRequest(TestUtils.toByteArray("hello"),
                       "abc".getBytes(),
                       TestUtils.getClock(1, 2, 2, 3),
                       true);
        testGetRequest(TestUtils.toByteArray("hello"),
                       "abcasdf".getBytes(),
                       TestUtils.getClock(1, 3, 4, 5),
                       true);

    }

    public void testGetRequest(ByteArray key, byte[] value, VectorClock version, boolean isPresent)
            throws Exception {
        try {
            if(isPresent)
                store.put(key, Versioned.value(value, version));
            ByteArrayOutputStream getRequest = new ByteArrayOutputStream();
            this.clientWireFormat.writeGetRequest(new DataOutputStream(getRequest),
                                                  storeName,
                                                  key,
                                                  false);
            ByteArrayOutputStream getResponse = new ByteArrayOutputStream();
            this.serverWireFormat.handleRequest(inputStream(getRequest),
                                                new DataOutputStream(getResponse));
            List<Versioned<byte[]>> values = this.clientWireFormat.readGetResponse(inputStream(getResponse));
            if(isPresent) {
                assertEquals(1, values.size());
                Versioned<byte[]> v = values.get(0);
                assertEquals(version, v.getVersion());
                assertTrue(Arrays.equals(v.getValue(), value));
            } else {
                assertEquals(0, values.size());
            }
        } finally {
            this.store.deleteAll();
        }
    }

    public void testGetAllRequests() throws Exception {
        testGetAllRequest(new ByteArray[] {},
                          new byte[][] {},
                          new VectorClock[] {},
                          new boolean[] {});

        testGetAllRequest(new ByteArray[] { new ByteArray() },
                          new byte[][] { new byte[] {} },
                          new VectorClock[] { new VectorClock() },
                          new boolean[] { true });

        testGetAllRequest(new ByteArray[] { TestUtils.toByteArray("hello") },
                          new byte[][] { "world".getBytes() },
                          new VectorClock[] { new VectorClock() },
                          new boolean[] { true });

        testGetAllRequest(new ByteArray[] { TestUtils.toByteArray("hello"),
                TestUtils.toByteArray("holly") }, new byte[][] { "world".getBytes(),
                "cow".getBytes() }, new VectorClock[] { TestUtils.getClock(1, 1),
                TestUtils.getClock(1, 2) }, new boolean[] { true, false });
    }

    public void testGetAllRequest(ByteArray[] keys,
                                  byte[][] values,
                                  VectorClock[] versions,
                                  boolean[] isFound) throws Exception {
        try {
            for(int i = 0; i < keys.length; i++) {
                if(isFound[i])
                    store.put(keys[i], Versioned.value(values[i], versions[i]));
            }
            ByteArrayOutputStream getAllRequest = new ByteArrayOutputStream();
            this.clientWireFormat.writeGetAllRequest(new DataOutputStream(getAllRequest),
                                                     storeName,
                                                     Arrays.asList(keys),
                                                     false);
            ByteArrayOutputStream getAllResponse = new ByteArrayOutputStream();
            this.serverWireFormat.handleRequest(inputStream(getAllRequest),
                                                new DataOutputStream(getAllResponse));
            Map<ByteArray, List<Versioned<byte[]>>> found = this.clientWireFormat.readGetAllResponse(inputStream(getAllResponse));
            for(int i = 0; i < keys.length; i++) {
                if(isFound[i]) {
                    assertTrue(keys[i] + " is not in the found set.", found.containsKey(keys[i]));
                    assertEquals(1, found.get(keys[i]).size());
                    Versioned<byte[]> versioned = found.get(keys[i]).get(0);
                    assertEquals(versions[i], versioned.getVersion());
                    assertTrue(Arrays.equals(values[i], versioned.getValue()));
                } else {
                    assertTrue(keys[i] + " is in the found set but should not be.",
                               !found.containsKey(keys[i]));
                }
            }
        } finally {
            this.store.deleteAll();
        }
    }

    public void testPutRequests() throws Exception {
        testPutRequest(new ByteArray(), new byte[0], new VectorClock(), null);
        testPutRequest(TestUtils.toByteArray("hello"), "world".getBytes(), new VectorClock(), null);

        // test obsolete exception
        this.store.put(TestUtils.toByteArray("hello"), new Versioned<byte[]>("world".getBytes(),
                                                                             new VectorClock()));
        testPutRequest(TestUtils.toByteArray("hello"),
                       "world".getBytes(),
                       new VectorClock(),
                       ObsoleteVersionException.class);
    }

    public void testPutRequest(ByteArray key,
                               byte[] value,
                               VectorClock version,
                               Class<? extends VoldemortException> exception) throws Exception {
        try {
            ByteArrayOutputStream putRequest = new ByteArrayOutputStream();
            this.clientWireFormat.writePutRequest(new DataOutputStream(putRequest),
                                                  storeName,
                                                  key,
                                                  value,
                                                  version,
                                                  false);
            ByteArrayOutputStream putResponse = new ByteArrayOutputStream();
            this.serverWireFormat.handleRequest(inputStream(putRequest),
                                                new DataOutputStream(putResponse));
            this.clientWireFormat.readPutResponse(inputStream(putResponse));
            TestUtils.assertContains(this.store, key, value);
        } catch(IllegalArgumentException e) {
            throw e;
        } catch(Exception e) {
            assertEquals("Unexpected exception " + e.getClass().getName(), e.getClass(), exception);
        } finally {
            this.store.deleteAll();
        }
    }

    public void testDeleteRequests() throws Exception {
        // test pre-existing are deleted
        testDeleteRequest(new ByteArray(),
                          new VectorClock(),
                          new Versioned<byte[]>("hello".getBytes()),
                          true);
        testDeleteRequest(TestUtils.toByteArray("hello"),
                          new VectorClock(),
                          new Versioned<byte[]>("world".getBytes()),
                          true);

        // test non-existant aren't deleted
        testDeleteRequest(TestUtils.toByteArray("hello"), new VectorClock(), null, false);
    }

    public void testDeleteRequest(ByteArray key,
                                  VectorClock version,
                                  Versioned<byte[]> existingValue,
                                  boolean isDeleted) throws Exception {
        try {
            if(existingValue != null)
                this.store.put(key, existingValue);
            ByteArrayOutputStream delRequest = new ByteArrayOutputStream();
            this.clientWireFormat.writeDeleteRequest(new DataOutputStream(delRequest),
                                                     storeName,
                                                     key,
                                                     version,
                                                     false);
            ByteArrayOutputStream delResponse = new ByteArrayOutputStream();
            this.serverWireFormat.handleRequest(inputStream(delRequest),
                                                new DataOutputStream(delResponse));
            boolean wasDeleted = this.clientWireFormat.readDeleteResponse(inputStream(delResponse));
            assertEquals(isDeleted, wasDeleted);
        } finally {
            this.store.deleteAll();
        }
    }

    public DataInputStream inputStream(ByteArrayOutputStream output) {
        return new DataInputStream(new ByteArrayInputStream(output.toByteArray()));
    }

}
