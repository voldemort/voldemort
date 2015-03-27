package voldemort.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormat;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.RequestHandler;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public abstract class AbstractRequestFormatTest extends TestCase {

    private final String storeName;
    private final RequestFormat clientWireFormat;
    private final RequestHandler serverWireFormat;
    private final InMemoryStorageEngine<ByteArray, byte[], byte[]> store;
    private final RequestFormatType type;

    public AbstractRequestFormatTest(RequestFormatType type) {
        this.type = type;
        this.storeName = "test";
        this.store = new InMemoryStorageEngine<ByteArray, byte[], byte[]>(storeName);
        StoreRepository repository = new StoreRepository();
        repository.addLocalStore(store);
        repository.addRoutedStore(store);
        this.clientWireFormat = new RequestFormatFactory().getRequestFormat(type);
        this.serverWireFormat = ServerTestUtils.getSocketRequestHandlerFactory(repository)
                                               .getRequestHandler(type);
    }

    @Test
    public void testNullKeys() throws Exception {
        try {
            testGetRequest(null, null, null, null, false);
            fail("Null key allowed.");
        } catch(IllegalArgumentException e) {
            // this is good
        }
        try {
            testGetAllRequest(new ByteArray[] { null }, null, null, null, new boolean[] { false });
            fail("Null key allowed.");
        } catch(IllegalArgumentException e) {
            // this is good
        }
        try {
            testPutRequest(null, null, null, null, null, true);
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

    @Test
    public void testGetRequests() throws Exception {
        testGetRequest(TestUtils.toByteArray("hello"), null, null, null, false);
        testGetRequest(TestUtils.toByteArray("hello"), "".getBytes(), null, new VectorClock(), true);
        testGetRequest(TestUtils.toByteArray("hello"),
                       "abc".getBytes(),
                       null,
                       TestUtils.getClock(1, 2, 2, 3),
                       true);
        testGetRequest(TestUtils.toByteArray("hello"),
                       "abcasdf".getBytes(),
                       null,
                       TestUtils.getClock(1, 3, 4, 5),
                       true);

    }

    public void testGetRequest(ByteArray key,
                               byte[] value,
                               byte[] transforms,
                               VectorClock version,
                               boolean isPresent) throws Exception {
        try {
            if(isPresent) {
                testPutRequest(key, value, null, version, null, false);
            }
            ByteArrayOutputStream getRequest = new ByteArrayOutputStream();
            this.clientWireFormat.writeGetRequest(new DataOutputStream(getRequest),
                                                  storeName,
                                                  key,
                                                  transforms,
                                                  RequestRoutingType.NORMAL);

            testIsCompleteRequest(getRequest);

            ByteArrayOutputStream getResponse = handleRequest(getRequest);
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

    // @Test
    public void testGetVersionRequest() throws Exception {
        testGetVersionRequest(TestUtils.toByteArray("hello"), null, null, false);
        testGetVersionRequest(TestUtils.toByteArray("hello"),
                              "".getBytes(),
                              new VectorClock(),
                              true);
        testGetVersionRequest(TestUtils.toByteArray("hello"),
                       "abc".getBytes(),
                       TestUtils.getClock(1, 2, 2, 3),
                       true);
        testGetVersionRequest(TestUtils.toByteArray("hello"),
                       "abcasdf".getBytes(),
                       TestUtils.getClock(1, 3, 4, 5),
                       true);

    }

    public void testGetVersionRequest(ByteArray key,
                                      byte[] value,
                                      VectorClock version,
                                      boolean isPresent) throws Exception {
        try {
            if(isPresent) {
                testPutRequest(key, value, null, version, null, false);
            }
            ByteArrayOutputStream getVersionRequest = new ByteArrayOutputStream();
            this.clientWireFormat.writeGetVersionRequest(new DataOutputStream(getVersionRequest),
                                                  storeName,
                                                  key,
                                                  RequestRoutingType.NORMAL);

            testIsCompleteRequest(getVersionRequest);

            ByteArrayOutputStream getVersionResponse = handleRequest(getVersionRequest);
            List<Version> values = this.clientWireFormat.readGetVersionResponse(inputStream(getVersionResponse));
            if(isPresent) {
                assertEquals(1, values.size());
                VectorClock returnValue = (VectorClock) values.get(0);
                assertEquals(version, returnValue);
                assertEquals(version.getTimestamp(), returnValue.getTimestamp());
            } else {
                assertEquals(0, values.size());
            }
        } finally {
            this.store.deleteAll();
        }
    }

    @Test
    public void testGetAllRequests() throws Exception {
        testGetAllRequest(new ByteArray[] {},
                          new byte[][] {},
                          null,
                          new VectorClock[] {},
                          new boolean[] {});

        testGetAllRequest(new ByteArray[] { new ByteArray() },
                          new byte[][] { new byte[] {} },
                          null,
                          new VectorClock[] { new VectorClock() },
                          new boolean[] { true });

        testGetAllRequest(new ByteArray[] { TestUtils.toByteArray("hello") },
                          new byte[][] { "world".getBytes() },
                          null,
                          new VectorClock[] { new VectorClock() },
                          new boolean[] { true });

        testGetAllRequest(new ByteArray[] { TestUtils.toByteArray("hello"),
                                  TestUtils.toByteArray("holly") },
                          new byte[][] { "world".getBytes(), "cow".getBytes() },
                          null,
                          new VectorClock[] { TestUtils.getClock(1, 1), TestUtils.getClock(1, 2) },
                          new boolean[] { true, false });
    }

    public void testGetAllRequest(ByteArray[] keys,
                                  byte[][] values,
                                  Map<ByteArray, byte[]> transforms,
                                  VectorClock[] versions,
                                  boolean[] isFound) throws Exception {
        try {
            for(int i = 0; i < keys.length; i++) {
                if(isFound[i])
                    testPutRequest(keys[i], values[i], null, versions[i], null, false);
            }
            ByteArrayOutputStream getAllRequest = new ByteArrayOutputStream();
            this.clientWireFormat.writeGetAllRequest(new DataOutputStream(getAllRequest),
                                                     storeName,
                                                     Arrays.asList(keys),
                                                     transforms,
                                                     RequestRoutingType.NORMAL);
            testIsCompleteRequest(getAllRequest);

            ByteArrayOutputStream getAllResponse = handleRequest(getAllRequest);
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

    @Test
    public void testPutRequests() throws Exception {
        testPutRequest(new ByteArray(), new byte[0], null, new VectorClock(), null, true);
        testPutRequest(TestUtils.toByteArray("hello"),
                       "world".getBytes(),
                       null,
                       new VectorClock(),
                       null,
                       false);
        testPutRequest(TestUtils.toByteArray("hello"),
                       "world".getBytes(),
                       null,
                       new VectorClock(),
                       ObsoleteVersionException.class,
                       true);
    }

    public void testPutRequest(ByteArray key,
                               byte[] value,
                               byte[] transforms,
                               VectorClock version,
                               Class<? extends VoldemortException> exception,
                               boolean deleteFinally) throws Exception {
        try {
            ByteArrayOutputStream putRequest = new ByteArrayOutputStream();
            this.clientWireFormat.writePutRequest(new DataOutputStream(putRequest),
                                                  storeName,
                                                  key,
                                                  value,
                                                  transforms,
                                                  version,
                                                  RequestRoutingType.NORMAL);

            testIsCompleteRequest(putRequest);
            ByteArrayOutputStream putResponse = handleRequest(putRequest);
            this.clientWireFormat.readPutResponse(inputStream(putResponse));
            TestUtils.assertContains(this.store, key, value);
        } catch(IllegalArgumentException e) {
            throw e;
        } catch(Exception e) {
            assertEquals("Unexpected exception " + e.getClass().getName(), e.getClass(), exception);
        } finally {
            if(deleteFinally) {
                this.store.deleteAll();
            }
        }
    }

    @Test
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
            if(existingValue != null) {
                testPutRequest(key,
                               existingValue.getValue(),
                               null,
                               (VectorClock) existingValue.getVersion(),
                               null,
                               false);
            }
            ByteArrayOutputStream delRequest = new ByteArrayOutputStream();
            this.clientWireFormat.writeDeleteRequest(new DataOutputStream(delRequest),
                                                     storeName,
                                                     key,
                                                     version,
                                                     RequestRoutingType.NORMAL);
            testIsCompleteRequest(delRequest);
            ByteArrayOutputStream delResponse = handleRequest(delRequest);
            boolean wasDeleted = this.clientWireFormat.readDeleteResponse(inputStream(delResponse));
            assertEquals(isDeleted, wasDeleted);
        } finally {
            this.store.deleteAll();
        }
    }

    private ByteArrayOutputStream handleRequest(ByteArrayOutputStream input) throws Exception {

        ByteArrayOutputStream response = new ByteArrayOutputStream();
        this.serverWireFormat.handleRequest(inputStream(input), new DataOutputStream(response));

        return response;
    }

    public DataInputStream inputStream(ByteArrayOutputStream output) {
        return new DataInputStream(new ByteArrayInputStream(output.toByteArray()));
    }

    public void testIsCompleteRequest(ByteArrayOutputStream request) {
        ByteBuffer buffer = ByteBuffer.wrap(request.toByteArray());
        int limit = buffer.limit();
        for(int i = 0; i < limit; i++) {
            buffer.limit(i);
            boolean isCompleteRequest = this.serverWireFormat.isCompleteRequest(buffer);
            assertFalse(" Partial requests should be inComplete", isCompleteRequest);
        }
        buffer.limit(limit);
        boolean isCompleteRequest = this.serverWireFormat.isCompleteRequest(buffer);
        assertFalse(" Full request should be complete", isCompleteRequest);
    }

}
