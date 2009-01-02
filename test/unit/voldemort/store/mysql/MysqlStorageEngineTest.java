package voldemort.store.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;

import voldemort.store.StorageEngine;
import voldemort.store.StorageEngineTest;

public class MysqlStorageEngineTest extends StorageEngineTest {
    
    private MysqlStorageEngine engine;
    
    public void setUp() throws Exception {
        this.engine = (MysqlStorageEngine) getStorageEngine();
        engine.destroy();
        engine.create();
        super.setUp();
    }

    @Override
    public StorageEngine<byte[],byte[]> getStorageEngine() {
        return new MysqlStorageEngine("test_store", getDataSource());
    }
    
    public void tearDown() {
        engine.destroy();
    }
    
    private DataSource getDataSource() {
        BasicDataSource ds = new BasicDataSource();
        ds.setUrl("jdbc:mysql://localhost:3306/test");
        ds.setUsername("root");
        ds.setPassword("");
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        return ds;
    }
    
    public void executeQuery(DataSource datasource, String query) throws SQLException {
        Connection c = datasource.getConnection();
        PreparedStatement s = c.prepareStatement(query);
        s.execute();
    }

}
