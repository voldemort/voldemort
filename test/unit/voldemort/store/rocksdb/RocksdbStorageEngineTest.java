package voldemort.store.rocksdb;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.io.FileDeleteStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.TestUtils;
import voldemort.server.VoldemortConfig;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

@RunWith(Parameterized.class)
public class RocksdbStorageEngineTest extends AbstractStorageEngineTest {

    RocksDbStorageConfiguration rocksDbConfig;
    RocksDbStorageEngine rocksDbStore;
    Random random;
    VoldemortConfig voldemortConfig;
    private boolean prefixPartitionId;

    public RocksdbStorageEngineTest(boolean prefixPartitionId) {
        this.prefixPartitionId = prefixPartitionId;
    }

    @Override
    public StorageEngine<ByteArray, byte[], byte[]> getStorageEngine() {
        return this.rocksDbStore;
    }

    @Parameters
    public static Collection<Object[]> modes() {
        Object[][] data = new Object[][] { { true }, { false } };
        return Arrays.asList(data);
    }

    @Override
    @Before
    public void setUp() {
        Properties props = new Properties();

        // Doesnt matter what node.id and voldemort.home values are for this
        // test
        props.setProperty("node.id", "0");
        props.setProperty("voldemort.home", "tmp/voldemort");

        voldemortConfig = new VoldemortConfig(props);
        if(this.prefixPartitionId) {
            voldemortConfig.setRocksdbPrefixKeysWithPartitionId(true);
        }
        this.rocksDbConfig = new RocksDbStorageConfiguration(voldemortConfig);
        this.rocksDbStore = (RocksDbStorageEngine) rocksDbConfig.getStore(TestUtils.makeStoreDefinition("test"),
                                                                          TestUtils.makeSingleNodeRoutingStrategy());
        random = new Random();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            rocksDbConfig.close();
        } finally {
            /*
             * Make sure to blow the data directory after every test to have a
             * clean set up for the next one.
             */
            File datadir = new File(this.voldemortConfig.getRdbDataDirectory() + "/test");
            FileDeleteStrategy.FORCE.delete(datadir);
        }
    }
}
