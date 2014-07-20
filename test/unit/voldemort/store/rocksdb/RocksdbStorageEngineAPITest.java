package voldemort.store.rocksdb;

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
import voldemort.versioning.Versioned;

public class RocksdbStorageEngineAPITest {

    RocksDbStorageConfiguration rocksDbConfig;
    RocksDbStorageEngine rocksDbStore;
    Random random;

    @Before
    public void setup() {
        Properties props = new Properties();

        // Doesnt matter what node.id and voldemort.home values are for this
        // test
        props.setProperty("node.id", "0");
        props.setProperty("voldemort.home", "tmp/voldemort");

        VoldemortConfig config = new VoldemortConfig(props);
        this.rocksDbConfig = new RocksDbStorageConfiguration(config);
        this.rocksDbStore = (RocksDbStorageEngine) rocksDbConfig.getStore(TestUtils.makeStoreDefinition("test"),
                                                                          TestUtils.makeSingleNodeRoutingStrategy());
        random = new Random();
    }

    @After
    public void tearDown() {
        rocksDbConfig.close();
    }

    @Test
    public void getAfterPut() {
        // generate random bytes for key
        byte[] keyBytes = new byte[100];
        random.nextBytes(keyBytes);
        ByteArray key = new ByteArray(keyBytes);

        // generate random bytes for values
        byte[] value = new byte[100];
        random.nextBytes(value);
        Versioned<byte[]> versionedValue = new Versioned<byte[]>(value);

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
}
