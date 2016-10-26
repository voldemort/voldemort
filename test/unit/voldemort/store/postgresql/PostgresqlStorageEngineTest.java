package voldemort.store.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;

import voldemort.TestUtils;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

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
        return new PostgresqlStorageEngine("test_store", getDataSource());
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
        new PostgresqlStorageEngine(newStore, getDataSource());
        DataSource ds = getDataSource();
        executeQuery(ds, "select 1 from " + newStore + " limit 1");
        executeQuery(ds, "drop table " + newStore);
    }

}
