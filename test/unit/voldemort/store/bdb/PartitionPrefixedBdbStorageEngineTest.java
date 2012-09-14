package voldemort.store.bdb;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileDeleteStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.store.StoreBinaryFormat;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

/**
 * Tests for the BDB storage engine prefixing the partition id to the keys, to
 * enable efficient partition scans
 * 
 * 
 * 
 */
public class PartitionPrefixedBdbStorageEngineTest {

    private File bdbMasterDir;
    private BdbStorageConfiguration bdbStorage;

    @Before
    public void setUp() throws Exception {
        bdbMasterDir = TestUtils.createTempDir();
        FileDeleteStrategy.FORCE.delete(bdbMasterDir);
        // lets use all the default values.
        Props props = new Props();
        props.put("node.id", 1);
        props.put("voldemort.home", "test/common/voldemort/config");
        VoldemortConfig voldemortConfig = new VoldemortConfig(props);
        voldemortConfig.setBdbCacheSize(10 * 1024 * 1024);
        voldemortConfig.setBdbOneEnvPerStore(true);
        voldemortConfig.setBdbDataDirectory(bdbMasterDir.toURI().getPath());
        voldemortConfig.setBdbPrefixKeysWithPartitionId(true);
        bdbStorage = new BdbStorageConfiguration(voldemortConfig);
    }

    @After
    public void tearDown() throws Exception {
        try {
            if(bdbStorage != null)
                bdbStorage.close();
        } finally {
            FileDeleteStrategy.FORCE.delete(bdbMasterDir);
        }
    }

    @Test
    public void testPartitionToByteArrayConversion() {
        // check the conversions used in the partition scan code path
        for(int i = 0; i <= ClusterMapper.MAX_PARTITIONID; i++) {
            byte[] pkey = StoreBinaryFormat.makePartitionKey(i);
            int j = StoreBinaryFormat.extractPartition(pkey);
            assertEquals(i, j);
        }

        byte[] key = "abcdefghijklmnopqrstuvwxyz".getBytes();
        // check the conversions used in the other code path
        byte[] prefixedkey = StoreBinaryFormat.makePrefixedKey(key, 20);
        int partition = StoreBinaryFormat.extractPartition(prefixedkey);
        assertEquals(partition, 20);
        assertEquals(0, ByteUtils.compare(key, StoreBinaryFormat.extractKey(prefixedkey)));
    }

    private Set<String> getKeys(ClosableIterator<ByteArray> itr) {
        HashSet<String> keySet = new HashSet<String>();
        while(itr.hasNext()) {
            keySet.add(new String(itr.next().get()));
        }
        itr.close();
        return keySet;
    }

    private Set<String> getEntries(ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> itr) {
        HashSet<String> keySet = new HashSet<String>();
        while(itr.hasNext()) {
            Pair<ByteArray, Versioned<byte[]>> entry = itr.next();
            ByteArray key = entry.getFirst();
            byte[] value = entry.getSecond().getValue();

            String skey = new String(key.get());
            int keyId = Integer.parseInt(skey.replaceAll("key", ""));
            assertEquals(0, ByteUtils.compare(value, ("value" + keyId).getBytes()));

            keySet.add(skey);
        }
        itr.close();
        return keySet;
    }

    @Test
    public void testPartitionScan() {

        StoreDefinition storedef = TestUtils.makeStoreDefinition("storeA");
        RoutingStrategy strategy = TestUtils.makeSingleNodeRoutingStrategy();
        BdbStorageEngine prefixedBdbStore = (BdbStorageEngine) bdbStorage.getStore(storedef,
                                                                                   strategy);
        try {
            // insert a bunch of records
            HashMap<Integer, Set<String>> partitionToKeysMap = new HashMap<Integer, Set<String>>();
            for(int i = 0; i < 10000; i++) {
                String key = "key" + i;
                byte[] bkey = key.getBytes();

                int partition = strategy.getPartitionList(bkey).get(0);
                if(!partitionToKeysMap.containsKey(partition))
                    partitionToKeysMap.put(partition, new HashSet<String>());
                partitionToKeysMap.get(partition).add(key);

                prefixedBdbStore.put(new ByteArray(bkey),
                                     new Versioned<byte[]>(("value" + i).getBytes()),
                                     null);
            }

            // check if they are properly retrieved by that partition id
            for(int p = 0; p < strategy.getNumReplicas(); p++) {

                // verify keys
                Set<String> keys = getKeys(prefixedBdbStore.keys(p));
                assertEquals(partitionToKeysMap.get(p).size(), keys.size());
                assertEquals(partitionToKeysMap.get(p), keys);

                // verfiy values
                keys = getEntries(prefixedBdbStore.entries(p));
                assertEquals(partitionToKeysMap.get(p).size(), keys.size());
                assertEquals(partitionToKeysMap.get(p), keys);
            }

            // make sure the entries() code path does not break.
            HashSet<String> allKeys = new HashSet<String>();
            for(Integer p: partitionToKeysMap.keySet()) {
                Set<String> pkeys = partitionToKeysMap.get(p);
                int originalSize = allKeys.size();
                allKeys.removeAll(pkeys);
                // this is to make sure the pkeys have 0 overlap
                assertEquals(allKeys.size(), originalSize);
                allKeys.addAll(pkeys);
            }
            // this makes sure all the data we put in is what you get out, not a
            // byte less or more
            Set<String> keys = getKeys(prefixedBdbStore.keys());
            assertEquals(allKeys.size(), keys.size());
            assertEquals(allKeys, keys);

            keys = getEntries(prefixedBdbStore.entries());
            assertEquals(allKeys.size(), keys.size());
            assertEquals(allKeys, keys);

        } catch(Exception e) {
            fail("Should not have thrown any exceptions" + e.getMessage());
        } finally {
            prefixedBdbStore.close();
        }
    }
}
