package voldemort.store.mysql;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;

import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StorageEngineType;

public class MysqlStorageConfiguration implements StorageConfiguration {

    private DataSource dataSource;

    public MysqlStorageConfiguration(VoldemortConfig config) {
        BasicDataSource ds = new BasicDataSource();
        ds.setUrl("jdbc:mysql://" + config.getMysqlHost() + ":" + config.getMysqlPort() + "/"
                  + config.getMysqlDatabaseName());
        ds.setUsername(config.getMysqlUsername());
        ds.setPassword(config.getMysqlPassword());
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        this.dataSource = ds;
    }

    public StorageEngine<byte[], byte[]> getStore(String name) {
        return new MysqlStorageEngine(name, dataSource);
    }

    public StorageEngineType getType() {
        return StorageEngineType.MYSQL;
    }

    public void close() {}

}
