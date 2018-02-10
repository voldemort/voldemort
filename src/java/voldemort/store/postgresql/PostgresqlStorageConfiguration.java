package voldemort.store.postgresql;

import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;

import voldemort.VoldemortException;
import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;

public class PostgresqlStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "postgresql";
    public static final int BATCH_HARD_LIMIT = 1000000;
    private final int batchSize;

    private BasicDataSource dataSource;

    public PostgresqlStorageConfiguration(final VoldemortConfig config) {
        BasicDataSource ds = new BasicDataSource();
        ds.setUrl("jdbc:postgresql://" + config.getPostgresHost() + ":" + config.getPostgresPort()
                  + "/" + config.getPostgresDatabaseName());
        ds.setUsername(config.getPostgresUsername());
        ds.setPassword(config.getPostgresPassword());
        ds.setDriverClassName("org.postgresql.Driver");
        batchSize = config.getPostgresbatchSize();

        this.dataSource = ds;
    }

    @Override
    public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef,
                                                             RoutingStrategy strategy) {
        return new PostgresqlStorageEngine(storeDef.getName(),
                                           dataSource,
                                           batchSize,
                                           BATCH_HARD_LIMIT);
    }

    @Override
    public String getType() {
        return TYPE_NAME;
    }

    @Override
    public void update(StoreDefinition storeDef) {
        throw new VoldemortException("Storage config updates not permitted for "
                                     + this.getClass().getCanonicalName());
    }

    @Override
    public void close() {
        try {
            this.dataSource.close();
        } catch(SQLException e) {
            throw new VoldemortException("Exception while closing connection pool.", e);
        }
    }

    // Nothing to do here: we're not tracking the created storage engine.
    @Override
    public void removeStorageEngine(StorageEngine<ByteArray, byte[], byte[]> engine) {}

}
