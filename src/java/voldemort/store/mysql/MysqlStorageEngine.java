/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

/**
 * A StorageEngine that uses Mysql for persistence
 * 
 * 
 */
public class MysqlStorageEngine implements StorageEngine<ByteArray, byte[], byte[]> {

    private static final Logger logger = Logger.getLogger(MysqlStorageEngine.class);
    private static int MYSQL_ERR_DUP_KEY = 1022;
    private static int MYSQL_ERR_DUP_ENTRY = 1062;

    private final String name;
    private final String valueType;
    private final DataSource datasource;

    public MysqlStorageEngine(final String name, final DataSource datasource, final String valueType) {
        this.name = name;
        this.datasource = datasource;
        this.valueType = valueType;

        if(!tableExists()) {
            create();
        }
    }

    private boolean tableExists() {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String select = "show tables like '" + getName() + "'";
        try {
            conn = this.datasource.getConnection();
            stmt = conn.prepareStatement(select);
            rs = stmt.executeQuery();
            return rs.next();
        } catch(SQLException e) {
            throw new PersistenceFailureException("SQLException while checking for table existence!",
                                                  e);
        } finally {
            tryClose(rs);
            tryClose(stmt);
            tryClose(conn);
        }
    }

    public void destroy() {
        execute("drop table if exists " + getName());
    }

    public void create() {
        execute("create table " + getName()
                + " (key_ varbinary(200) not null, version_ varbinary(200) not null, "
                + " value_ " + valueType + ", primary key(key_, version_)) engine = InnoDB");
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

    public ClosableIterator<ByteArray> keys() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String select = "select key_ from " + name;
        try {
            conn = datasource.getConnection();
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            rs = stmt.executeQuery(select);
            return new MysqlKeyIterator(conn, stmt, rs);
        } catch(SQLException e) {
        	logger.error(e.getMessage());
            throw new PersistenceFailureException(e.getMessage(), e);
        }
    }

    public void truncate() {
        Connection conn = null;
        PreparedStatement stmt = null;
        String select = "delete from " + name;
        try {
            conn = datasource.getConnection();
            stmt = conn.prepareStatement(select);
            stmt.executeUpdate();
        } catch(SQLException e) {
        	logger.error(e.getMessage());
            throw new PersistenceFailureException(e.getMessage(), e)
        } finally {
            tryClose(stmt);
            tryClose(conn);
        }
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String select = "select key_, version_, value_ from " + name;

        try {
            conn = datasource.getConnection();
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            rs = stmt.executeQuery(select);
            return new MysqlEntryIterator(conn, stmt, rs);
        } catch(SQLException e) {
        	logger.error(e.getMessage());
            throw new PersistenceFailureException(e.getMessage(), e);
        
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(int partition) {
        throw new UnsupportedOperationException("Partition based entries scan not supported for this storage type");
    }

    public ClosableIterator<ByteArray> keys(int partition) {
        throw new UnsupportedOperationException("Partition based key scan not supported for this storage type");
    }

    public void close() throws PersistenceFailureException {
        // don't close datasource cause others could be using it
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    public boolean delete(ByteArray key, Version maxVersion) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
        Connection conn = null;
        PreparedStatement selectStmt = null;
        ResultSet rs = null;
        String select = "select key_, version_ from " + name + " where key_ = ? for update";

        try {
            conn = datasource.getConnection();
            selectStmt = conn.prepareStatement(select);
            selectStmt.setBytes(1, key.get());
            rs = selectStmt.executeQuery();
            boolean deletedSomething = false;
            while(rs.next()) {
                byte[] theKey = rs.getBytes("key_");
                byte[] version = rs.getBytes("version_");
                if(new VectorClock(version).compare(maxVersion) == Occurred.BEFORE) {
                    delete(conn, theKey, version);
                    deletedSomething = true;
                }
            }

            return deletedSomething;
        } catch(SQLException e) {
        	logger.error(e.getMessage());
            throw new PersistenceFailureException(e.getMessage(), e);
        } finally {
            tryClose(rs);
            tryClose(selectStmt);
            tryClose(conn);
        }
    }

    private void delete(Connection connection, byte[] key, byte[] version) throws SQLException {
        String delete = "delete from " + name + " where key_ = ? and version_ = ?";
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

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String select = "select version_, value_ from " + name + " where key_ = ?";
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
        	logger.error(e.getMessage());
            throw new PersistenceFailureException(e.getMessage(), e);
        } finally {
            tryClose(rs);
            tryClose(stmt);
            tryClose(conn);
        }
    }

    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms)
            throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
        return StoreUtils.get(this, key, transforms);
    }

    public String getName() {
        return name;
    }

    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
        boolean doCommit = false;
        Connection conn = null;
        PreparedStatement insert = null;
        PreparedStatement select = null;
        ResultSet results = null;
        String insertSql = "insert into " + name + " (key_, version_, value_) values (?, ?, ?)";
        String selectSql = "select key_, version_ from " + name + " where key_ = ?";
        try {
            conn = datasource.getConnection();
            conn.setAutoCommit(false);

            // check for superior versions
            select = conn.prepareStatement(selectSql);
            select.setBytes(1, key.get());
            results = select.executeQuery();
            while(results.next()) {
                byte[] thisKey = results.getBytes("key_");
                VectorClock version = new VectorClock(results.getBytes("version_"));
                Occurred occurred = value.getVersion().compare(version);
                if(occurred == Occurred.BEFORE)
                    throw new ObsoleteVersionException("Attempt to put version "
                                                       + value.getVersion()
                                                       + " which is superceeded by " + version
                                                       + ".");
                else if(occurred == Occurred.AFTER)
                    delete(conn, thisKey, version.toBytes());
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
            if(e.getErrorCode() == MYSQL_ERR_DUP_KEY || e.getErrorCode() == MYSQL_ERR_DUP_ENTRY) {
                throw new ObsoleteVersionException("Key or value already used.");
            } else {
                throw new PersistenceFailureException("Fix me!", e);
            }
        } finally {
            if(conn != null) {
                try {
                    if(doCommit)
                        conn.commit();
                    else
                        conn.rollback();
                } catch(SQLException e) {}
            }
            tryClose(results);
            tryClose(insert);
            tryClose(select);
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

    private abstract class MysqlClosableIterator<T> implements ClosableIterator<T> {

        boolean hasMore;
        final ResultSet rs;
        final Connection connection;
        final Statement statement;

        public MysqlClosableIterator(Connection connection, Statement statement, ResultSet resultSet) {
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

        public void close() {
            tryClose(rs);
            tryClose(statement);
            tryClose(connection);
        }

        public boolean hasNext() {
            return this.hasMore;
        }

        public abstract T next();

        public void remove() {
            try {
                rs.deleteRow();
            } catch(SQLException e) {
                throw new PersistenceFailureException(e);
            }
        }
    }

    private class MysqlKeyIterator extends MysqlClosableIterator<ByteArray> {

        public MysqlKeyIterator(Connection connection, Statement statement, ResultSet resultSet) {
            super(connection, statement, resultSet);
        }

        @Override
        public ByteArray next() {
            try {
                if(!this.hasMore)
                    throw new PersistenceFailureException("Next called on iterator, but no more items available!");
                ByteArray key = new ByteArray(rs.getBytes("key_"));
                this.hasMore = rs.next();
                return key;
            } catch(SQLException e) {
                throw new PersistenceFailureException(e);
            }
        }
    }

    private class MysqlEntryIterator extends
            MysqlClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

        public MysqlEntryIterator(Connection connection, Statement statement, ResultSet resultSet) {
            super(connection, statement, resultSet);
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
    }

    public List<Version> getVersions(ByteArray key) {
        return StoreUtils.getVersions(get(key, null));
    }

    public boolean isPartitionAware() {
        return false;
    }

    public boolean isPartitionScanSupported() {
        // no reason why we cannot do this on MySQL. Will be added later
        return false;
    }

    @Override
    public boolean beginBatchModifications() {
        return false;
    }

    @Override
    public boolean endBatchModifications() {
        return false;
    }
}
