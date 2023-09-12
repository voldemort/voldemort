package voldemort.store.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import voldemort.VoldemortException;
import voldemort.store.AbstractStorageEngine;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class PostgresqlStorageEngine extends AbstractStorageEngine<ByteArray, byte[], byte[]> {

    private static final Logger logger = Logger.getLogger(PostgresqlStorageEngine.class);
    private static int PG_ERR_DUP_KEY = 23505;
    private final int BATCH_HARD_LIMIT;
    private final int BATCH_SIZE;

    private final DataSource datasource;

    public PostgresqlStorageEngine(final String name,
                                   final DataSource datasource,
                                   final int batchSize,
                                   final int batchHardLimit) {
        super(name);
        this.datasource = datasource;

        this.BATCH_SIZE = batchSize;

        this.BATCH_HARD_LIMIT = batchHardLimit;

        // create does a create if not exists.
        this.create();

    }

    public void create() {
        execute("create table if not exists " + getName()
                + " (key_ bytea not null, version_ bytea not null, "
                + " value_ bytea, primary key(key_, version_))");
    }

    public void destroy() {
        execute("drop table if exists " + getName());
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String select = "select key_, version_, value_ from " + getName();
        try {
            conn = datasource.getConnection();
            stmt = conn.prepareStatement(select);
            rs = stmt.executeQuery();
            return new PGClosableIterator(conn, stmt, rs);
        } catch(SQLException e) {
            throw new PersistenceFailureException("Fix me!", e);
        }
    }

    @Override
    public boolean delete(ByteArray key, Version maxVersion) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
        Connection conn = null;
        PreparedStatement selectStmt = null;
        ResultSet rs = null;
        String select = "select version_ from " + getName() + " where key_ = ? for update";

        try {
            conn = datasource.getConnection();
            selectStmt = conn.prepareStatement(select);
            selectStmt.setBytes(1, key.get());
            rs = selectStmt.executeQuery();
            boolean deletedSomething = false;
            while(rs.next()) {
                byte[] version = rs.getBytes("version_");
                if((maxVersion == null)
                   || (new VectorClock(version).compare(maxVersion) == Occurred.BEFORE)) {
                    delete(conn, key.get(), version);
                    deletedSomething = true;
                }
            }

            return deletedSomething;
        } catch(SQLException e) {
            throw new PersistenceFailureException("Fix me!", e);
        } finally {
            tryClose(rs);
            tryClose(selectStmt);
            tryClose(conn);
        }
    }

    private void delete(Connection connection, byte[] key, byte[] version) throws SQLException {
        String delete = "delete from " + getName() + " where key_ = ? and version_ = ?";
        PreparedStatement deleteStmt = null;
        try {

            deleteStmt = connection.prepareStatement(delete);
            deleteStmt.setBytes(1, key);
            deleteStmt.setBytes(2, version);
            deleteStmt.executeUpdate();
        } finally {
            tryClose(deleteStmt);
        }
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
                                                                  throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String select = "select version_, value_ from " + getName() + " where key_ = ?";
        try {
            conn = datasource.getConnection();
            stmt = conn.prepareStatement(select);
            Map<ByteArray, List<Versioned<byte[]>>> result = StoreUtils.newEmptyHashMap(keys);
            for(ByteArray key: keys) {
                stmt.setBytes(1, key.get());
                rs = stmt.executeQuery();
                List<Versioned<byte[]>> found = Lists.newArrayList();
                while(rs.next()) {
                    byte[] version = rs.getBytes("version_");
                    byte[] value = rs.getBytes("value_");
                    found.add(new Versioned<byte[]>(value, new VectorClock(version)));
                }
                if(found.size() > 0)
                    result.put(key, found);
            }
            return result;
        } catch(SQLException e) {
            throw new PersistenceFailureException("Fix me!", e);
        } finally {
            tryClose(rs);
            tryClose(stmt);
            tryClose(conn);
        }
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms)
            throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
        return StoreUtils.get(this, key, transforms);
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
        boolean doCommit = false;
        Connection conn = null;
        PreparedStatement insert = null;
        PreparedStatement select = null;
        ResultSet results = null;
        String insertSql = "insert into " + getName()
                           + " (key_, version_, value_) values (?, ?, ?)";
        String selectSql = "select version_ from " + getName() + " where key_ = ?";
        try {
            conn = datasource.getConnection();
            conn.setAutoCommit(false);

            select = conn.prepareStatement(selectSql);
            select.setBytes(1, key.get());
            results = select.executeQuery();
            while(results.next()) {
                VectorClock version = new VectorClock(results.getBytes("version_"));
                Occurred occurred = value.getVersion().compare(version);
                if(occurred == Occurred.BEFORE)
                    throw new ObsoleteVersionException("Attempt to put version "
                                                       + value.getVersion()
                                                       + " which is superceeded by " + version
                                                       + ".");
                else if(occurred == Occurred.AFTER)
                    delete(conn, key.get(), version.toBytes());
            }

            // Okay, cool, now put the value
            insert = conn.prepareStatement(insertSql);
            insert.setBytes(1, key.get());
            VectorClock clock = (VectorClock) value.getVersion();
            insert.setBytes(2, clock.toBytes());
            insert.setBytes(3, value.getValue());
            insert.executeUpdate();
            doCommit = true;
        } catch(SQLException e) {
            if(e.getErrorCode() == PG_ERR_DUP_KEY) {
                throw new ObsoleteVersionException("Key or value already used.");
            } else {
                throw new PersistenceFailureException("Fix me!", e);
            }
        } finally {
            if(conn != null) {
                try {
                    if(doCommit) {
                        long currentTimeMillis4 = System.currentTimeMillis();
                        conn.commit();
                        long currentTimeMillis5 = System.currentTimeMillis();
                        // System.out.println("commit Time taken : " +
                        // (currentTimeMillis5 - currentTimeMillis4));
                    } else
                        conn.rollback();
                } catch(SQLException e) {}
            }
            tryClose(results);
            tryClose(insert);
            tryClose(select);
            tryClose(conn);
        }
    }

    @Override
    public void putAll(Map<ByteArray, Versioned<byte[]>> entries, byte[] transforms) {
        putBatch(entries, transforms, this.BATCH_SIZE);
    }

    private void putBatch(Map<ByteArray, Versioned<byte[]>> entries,
                         byte[] transforms,
                         int batchSize) {

        Connection conn = null;
        PreparedStatement insert = null;
        PreparedStatement select = null;
        boolean doCommit = false;

        String insertSql = "insert into " + getName()
                           + " (key_, version_, value_) values (?, ?, ?)";
        String selectSql = "select version_ from " + getName() + " where key_ = ?";
        try {
            conn = datasource.getConnection();
            conn.setAutoCommit(doCommit);
            select = conn.prepareStatement(selectSql);
            insert = conn.prepareStatement(insertSql);

            // all entries
            if(batchSize == -1) {
                if(entries.size() > BATCH_HARD_LIMIT) {
                    throw new UnsupportedOperationException("Resultant batchSize is greater than "
                                                            + BATCH_HARD_LIMIT);
                } else {
                    entries.entrySet().forEach(new PutConsumer(conn, insert, select));
                    doCommit = true;
                }
            } else if(batchSize > BATCH_HARD_LIMIT) {
                throw new UnsupportedOperationException("Resultant batchSize is greater than "
                                                        + BATCH_HARD_LIMIT);
            } else {
                if(batchSize > entries.size()) {
                    entries.entrySet().forEach(new PutConsumer(conn, insert, select));
                    doCommit = true;
                } else {
                    Set<Entry<ByteArray, Versioned<byte[]>>> batchedEntries = new HashSet<Entry<ByteArray, Versioned<byte[]>>>();
                    int k = 0;
                    int i = 0;
                    for(Entry<ByteArray, Versioned<byte[]>> entry: entries.entrySet()) {
                        if(k < entries.size() && i < batchSize) {
                            batchedEntries.add(entry);
                            k++;
                            i++;
                        }
                        if(k == entries.size()) {
                            batchedEntries.forEach(new PutConsumer(conn, insert, select));
                            break;
                        }
                        if(k < entries.size() && i == batchSize) {
                            batchedEntries.forEach(new PutConsumer(conn, insert, select));
                            i = 0;
                            batchedEntries.clear();
                        }
                    }
                    doCommit = true;
                }
            }
        } catch(SQLException se) {
            if(se.getErrorCode() == PG_ERR_DUP_KEY) {
                throw new ObsoleteVersionException("Key or value already used.");
            } else {
                throw new PersistenceFailureException("Failed to connect store !", se);
            }
        } finally {
            if(conn != null) {
                try {
                    if(doCommit)
                        conn.commit();
                    else
                        conn.rollback();
                } catch(SQLException e) {
                    e.printStackTrace();
                }
            }

            tryClose(insert);
            tryClose(select);
            tryClose(conn);
        }

    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(int partition) {
        throw new UnsupportedOperationException("Partition based entries scan not supported for this storage type");
    }

    @Override
    public ClosableIterator<ByteArray> keys(int partition) {
        throw new UnsupportedOperationException("Partition based key scan not supported for this storage type");
    }

    @Override
    public void close() throws PersistenceFailureException {
        // don't close datasource cause others could be using it
    }

    private class PutConsumer implements Consumer<Entry<ByteArray, Versioned<byte[]>>> {

        private final Connection conn;
        private final PreparedStatement insert;
        private final PreparedStatement select;
        private ResultSet results = null;

        public PutConsumer(Connection conn, PreparedStatement insert, PreparedStatement select) {
            this.conn = conn;
            this.insert = insert;
            this.select = select;
        }

        @Override
        public void accept(Entry<ByteArray, Versioned<byte[]>> t) {
            try {
                select.setBytes(1, t.getKey().get());
                results = select.executeQuery();
                while(results.next()) {
                    VectorClock version = new VectorClock(results.getBytes("version_"));
                    Occurred occurred = t.getValue().getVersion().compare(version);
                    if(occurred == Occurred.BEFORE)
                        throw new ObsoleteVersionException("Attempt to put version "
                                                           + t.getValue().getVersion()
                                                           + " which is superceeded by " + version
                                                           + ".");
                    else if(occurred == Occurred.AFTER)
                        delete(conn, t.getKey().get(), version.toBytes());
                }
                insert.setBytes(1, t.getKey().get());
                VectorClock clock = (VectorClock) t.getValue().getVersion();
                insert.setBytes(2, clock.toBytes());
                insert.setBytes(3, t.getValue().getValue());
                insert.executeUpdate();
            } catch(SQLException e) {
                if(e.getErrorCode() == PG_ERR_DUP_KEY) {
                    throw new ObsoleteVersionException("Key or value already used.");
                } else {
                    throw new PersistenceFailureException("Fix me!", e);
                }
            } finally {
                tryClose(results);
            }

        }

    }

    private class PGClosableIterator
            implements ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

        private boolean hasMore;
        private final ResultSet rs;
        private final Connection connection;
        private final PreparedStatement statement;

        public PGClosableIterator(Connection connection,
                                  PreparedStatement statement,
                                  ResultSet resultSet) {
            try {
                // Move to the first item
                this.hasMore = resultSet.next();
            } catch(SQLException e) {
                throw new PersistenceFailureException(e);
            }
            this.rs = resultSet;
            this.connection = connection;
            this.statement = statement;
        }

        @Override
        public void close() {
            tryClose(rs);
            tryClose(statement);
            tryClose(connection);
        }

        @Override
        public boolean hasNext() {
            return this.hasMore;
        }

        @Override
        public Pair<ByteArray, Versioned<byte[]>> next() {
            try {
                if(!this.hasMore)
                    throw new PersistenceFailureException("Next called on iterator, but no more items available!");
                ByteArray key = new ByteArray(rs.getBytes("key_"));
                byte[] value = rs.getBytes("value_");
                VectorClock clock = new VectorClock(rs.getBytes("version_"));
                this.hasMore = rs.next();
                return Pair.create(key, new Versioned<byte[]>(value, clock));
            } catch(SQLException e) {
                throw new PersistenceFailureException(e);
            }
        }

        @Override
        public void remove() {
            try {
                rs.deleteRow();
            } catch(SQLException e) {
                throw new PersistenceFailureException(e);
            }
        }

    }

    @Override
    public ClosableIterator<ByteArray> keys() {
        return StoreUtils.keys(entries());
    }

    @Override
    public void truncate() {
        execute("TRUNCATE TABLE " + getName());
    }

    public void execute(String query) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = datasource.getConnection();
            stmt = conn.prepareStatement(query);
            stmt.executeUpdate();
        } catch(SQLException e) {
            throw new PersistenceFailureException("SQLException while performing operation.", e);
        } finally {
            tryClose(stmt);
            tryClose(conn);
        }
    }

    private void tryClose(ResultSet rs) {
        try {
            if(rs != null)
                rs.close();
        } catch(Exception e) {
            logger.error("Failed to close resultset.", e);
        }
    }

    private void tryClose(Connection c) {
        try {
            if(c != null)
                c.close();
        } catch(Exception e) {
            logger.error("Failed to close connection.", e);
        }
    }

    private void tryClose(PreparedStatement s) {
        try {
            if(s != null)
                s.close();
        } catch(Exception e) {
            logger.error("Failed to close prepared statement.", e);
        }
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        return StoreUtils.getVersions(get(key, null));
    }

}
