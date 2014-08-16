package voldemort.store.rocksdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.server.VoldemortConfig;
import voldemort.store.PersistenceFailureException;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class RocksdbStorageEngineAPITest {

    RocksDbStorageConfiguration rocksDbConfig;
    RocksDbStorageEngine rocksDbStore;
    Random random;
    VoldemortConfig voldemortConfig;

    @Before
    public void setup() {
        Properties props = new Properties();

        // Doesnt matter what node.id and voldemort.home values are for this
        // test
        props.setProperty("node.id", "0");
        props.setProperty("voldemort.home", "tmp/voldemort");

        voldemortConfig = new VoldemortConfig(props);
        this.rocksDbConfig = new RocksDbStorageConfiguration(voldemortConfig);
        this.rocksDbStore = (RocksDbStorageEngine) rocksDbConfig.getStore(TestUtils.makeStoreDefinition("test"),
                                                                          TestUtils.makeSingleNodeRoutingStrategy());
        random = new Random();
    }

    @After
    public void tearDown() {
        rocksDbConfig.close();
    }

    protected ByteArray generateRandomKeys(int length) {
        // generate random bytes for key of given length
        byte[] keyBytes = new byte[length];
        random.nextBytes(keyBytes);
        return new ByteArray(keyBytes);
    }

    protected Versioned<byte[]> generateVersionedValue(int length) {
        // generate random bytes for values of given lenght
        byte[] value = new byte[length];
        random.nextBytes(value);
        return new Versioned<byte[]>(value);
    }

    @Test
    public void testGetAfterPut() {
        System.out.println("*********** testing get after put ***********");
        // generate random bytes for key
        ByteArray key = generateRandomKeys(100);

        // generate random bytes for values
        Versioned<byte[]> versionedValue = generateVersionedValue(100);

        try {
            // Do put
            this.rocksDbStore.put(key, versionedValue, null);
        } catch(PersistenceFailureException pfe) {
            Assert.fail("initial put failed unexpectedly. Exception: " + pfe.getMessage());
        }

        List<Versioned<byte[]>> found = null;
        // Do a get now to see if key exists
        try {
            found = this.rocksDbStore.get(key, null);
        } catch(PersistenceFailureException pfe) {
            Assert.fail("read after write unexpectedly throws Exception - " + pfe.getMessage());
        }

        if(found != null && found.size() > 0) {
            if(ByteUtils.compare(versionedValue.getValue(), found.get(0).getValue()) != 0) {
                Assert.fail("The returned value and the expected value is not same");
            }
        } else {
            Assert.fail("Either returned value is null or empty");
        }
    }

    public void testDelete(ByteArray key, Version versionToDelete) {
        System.out.println("version to delete: "
                           + (versionToDelete == null ? "null" : versionToDelete.toString()));
        List<Versioned<byte[]>> found = null;
        // Do a get now to see if key exists
        try {
            found = this.rocksDbStore.get(key, null);
        } catch(PersistenceFailureException pfe) {
            Assert.fail("read after write unexpectedly throws Exception - " + pfe.getMessage());
        }

        try {
            this.rocksDbStore.delete(key, versionToDelete);
        } catch(PersistenceFailureException pfe) {
            Assert.fail("delete operation on an existing key unexpectedly threw an exception - "
                        + pfe.getMessage());
        }

        try {
            found = this.rocksDbStore.get(key, null);
            if(versionToDelete == null) {
                // Expect the entire key must be deleted in case of a
                // unversioned delete
                if(found.size() > 0) {
                    Assert.fail("The key still has some value even after an unversioned delete");
                }
                return;
            }
            for(Versioned<byte[]> versionedValue1: found) {
                if(versionedValue1.getVersion().compare(versionToDelete) == Occurred.BEFORE) {
                    Assert.fail("Delete operation did not work as expected. The key still has versions lower than the specified version");
                }
            }
        } catch(PersistenceFailureException pfe) {
            Assert.fail("read after delete failed unexpectedly with the exception - "
                        + pfe.getMessage());
        }
    }

    @Test
    public void testUnversionedDelete() {
        System.out.println("*********** testing unversioned delete ***********");
        // generate random bytes for key
        ByteArray key = generateRandomKeys(100);

        // generate random bytes for values
        Versioned<byte[]> versionedValue = generateVersionedValue(100);

        try {
            // Do put
            this.rocksDbStore.put(key, versionedValue, null);
        } catch(PersistenceFailureException pfe) {
            Assert.fail("initial put failed unexpectedly. Exception: " + pfe.getMessage());
        }

        testDelete(key, null);
    }

    public List<Versioned<byte[]>> generatePutWithConflictionVersions(ByteArray key) {

        List<Versioned<byte[]>> versionedList = new ArrayList<Versioned<byte[]>>();

        VectorClock vectorClock1 = new VectorClock();
        vectorClock1.incrementVersion(voldemortConfig.getNodeId(), System.currentTimeMillis());
        Versioned<byte[]> value1 = new Versioned<byte[]>("valueOne".getBytes(), vectorClock1);
        try {
            // Do put
            this.rocksDbStore.put(key, value1, null);
        } catch(PersistenceFailureException pfe) {
            Assert.fail("initial put failed unexpectedly. Exception: " + pfe.getMessage());
        }
        versionedList.add(value1);

        VectorClock vectorClock2 = new VectorClock();
        vectorClock2.incrementVersion(1, System.currentTimeMillis());
        Versioned<byte[]> value2 = new Versioned<byte[]>("valueTwo".getBytes(), vectorClock2);
        try {
            // Do put
            this.rocksDbStore.put(key, value2, null);
        } catch(PersistenceFailureException pfe) {
            Assert.fail("initial put failed unexpectedly. Exception: " + pfe.getMessage());
        }
        versionedList.add(value2);
        return versionedList;
    }

    @Test
    public void testVersionedDelete() {
        System.out.println("*********** testing versioned delete ***********");
        ByteArray key = new ByteArray("keyOne".getBytes());
        List<Versioned<byte[]>> conflictingVersions = generatePutWithConflictionVersions(key);
        if(conflictingVersions.size() == 0) {
            Assert.fail("Could not generate conflicting versions ");
        }
        testDelete(key, conflictingVersions.get(0).getVersion());
    }

}
