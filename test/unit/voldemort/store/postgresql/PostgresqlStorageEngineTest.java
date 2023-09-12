package voldemort.store.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

public class PostgresqlStorageEngineTest extends AbstractStorageEngineTest {

    private PostgresqlStorageEngine engine;

    @Override
    public void setUp() throws Exception {
        this.engine = (PostgresqlStorageEngine) getStorageEngine();
        engine.destroy();
        engine.create();
        super.setUp();
    }

    @Override
    public StorageEngine<ByteArray, byte[], byte[]> getStorageEngine() {
        return new PostgresqlStorageEngine("test_store", getDataSource(), 10000, 10000);
    }

    @Override
    public void tearDown() {
        engine.destroy();
    }

    private DataSource getDataSource() {
        BasicDataSource ds = new BasicDataSource();
        ds.setUrl("jdbc:postgresql://localhost:5432/postgres");
        ds.setUsername("postgres");
        ds.setPassword("postgres");
        ds.setDriverClassName("org.postgresql.Driver");
        return ds;
    }

    public void executeQuery(DataSource datasource, String query) throws SQLException {
        Connection c = datasource.getConnection();
        PreparedStatement s = c.prepareStatement(query);
        s.execute();
    }

    public void testOpenNonExistantStoreCreatesTable() throws SQLException {
        String newStore = TestUtils.randomLetters(15);
        /* Create the engine for side-effect */
        new PostgresqlStorageEngine(newStore, getDataSource(), 10000, 100000);
        DataSource ds = getDataSource();
        executeQuery(ds, "select 1 from " + newStore + " limit 1");
        executeQuery(ds, "drop table " + newStore);
    }

    @Test
    public void testPutBatchSameBatchSizeAndHardLimit() {
        final int numPut = 10000;
        final StorageEngine<ByteArray, byte[], byte[]> store = new PostgresqlStorageEngine("test_store",
                                                                                           getDataSource(),
                                                                                           10000,
                                                                                           10000);
        Map<ByteArray, Versioned<byte[]>> input = new HashMap<ByteArray, Versioned<byte[]>>();
        for(int i = 0; i < numPut; i++) {
            String key = "key-" + i;
            String value = "Value for " + key;
            input.put(new ByteArray(key.getBytes()), new Versioned<byte[]>(value.getBytes()));
        }
        store.putAll(input, null);
        int numGet = 0;
        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> it = null;

        try {
            it = store.entries();
            while(it.hasNext()) {
                it.next();
                numGet++;
            }
            long endIter = System.currentTimeMillis();

        } finally {
            if(it != null) {
                it.close();
            }
        }
        assertEquals("Iterator returned by the call to entries() did not contain the expected number of values",
                     numPut,
                     numGet);
    }

    @Test
    public void testPutBatchSameBatchSizeLessThanHardLimit() {
        final int numPut = 10000;
        final StorageEngine<ByteArray, byte[], byte[]> store = new PostgresqlStorageEngine("test_store",
                                                                                           getDataSource(),
                                                                                           1000,
                                                                                           10000);
        Map<ByteArray, Versioned<byte[]>> input = new HashMap<ByteArray, Versioned<byte[]>>();
        for(int i = 0; i < numPut; i++) {
            String key = "key-" + i;
            String value = "Value for " + key;
            input.put(new ByteArray(key.getBytes()), new Versioned<byte[]>(value.getBytes()));
        }
        store.putAll(input, null);
        int numGet = 0;
        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> it = null;

        try {
            it = store.entries();
            while(it.hasNext()) {
                it.next();
                numGet++;
            }
            long endIter = System.currentTimeMillis();

        } finally {
            if(it != null) {
                it.close();
            }
        }
        assertEquals("Iterator returned by the call to entries() did not contain the expected number of values",
                     numPut,
                     numGet);
    }

    @Test
    public void testPutBatchSameBatchSizeGrtThanHardLimit() {
        final int numPut = 10000;
        final StorageEngine<ByteArray, byte[], byte[]> store = new PostgresqlStorageEngine("test_store",
                                                                                           getDataSource(),
                                                                                           10000,
                                                                                           1000);
        Map<ByteArray, Versioned<byte[]>> input = new HashMap<ByteArray, Versioned<byte[]>>();
        for(int i = 0; i < numPut; i++) {
            String key = "key-" + i;
            String value = "Value for " + key;
            input.put(new ByteArray(key.getBytes()), new Versioned<byte[]>(value.getBytes()));
        }
        try {
        store.putAll(input, null);
        } catch(Exception ex) {
            assertEquals(UnsupportedOperationException.class, ex.getClass());
        }
    }

}
