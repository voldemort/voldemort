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
import java.util.List;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import voldemort.store.Entry;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreUtils;
import voldemort.utils.ClosableIterator;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

/**
 * A StorageEngine that uses Mysql for persistence
 * 
 * @author jay
 * 
 */
public class MysqlStorageEngine implements StorageEngine<byte[], byte[]> {

    private static final Logger logger = Logger.getLogger(MysqlStorageEngine.class);
    private static int MYSQL_ERR_DUP_KEY = 1022;
    private static int MYSQL_ERR_DUP_ENTRY = 1062;

    private final String name;
    private final DataSource datasource;

    public MysqlStorageEngine(String name, DataSource datasource) {
        this.name = name;
        this.datasource = datasource;

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
            throw new PersistenceFailureException("Fix me!", e);
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
        execute("create table "
                + getName()
                + " (key_ varbinary(200) not null, version_ varbinary(200) not null, value_ blob, primary key(key_, version_))");
    }

    public void execute(String query) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = datasource.getConnection();
            stmt = conn.prepareStatement(query);
            stmt.executeUpdate();
        } catch(SQLException e) {
            throw new PersistenceFailureException("Fix me!", e);
        } finally {
            tryClose(stmt);
            tryClose(conn);
        }
    }

    public ClosableIterator<Entry<byte[], Versioned<byte[]>>> entries() {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String select = "select key_, version_, value_ from " + name;
        try {
            conn = datasource.getConnection();
            stmt = conn.prepareStatement(select);
            rs = stmt.executeQuery();
            return new MysqlClosableIterator(conn, stmt, rs);
        } catch(SQLException e) {
            throw new PersistenceFailureException("Fix me!", e);
        }
    }

    public void close() throws PersistenceFailureException {
    // don't close datasource cause others could be using it
    }

    public boolean delete(byte[] key, Version maxVersion) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
        Connection conn = null;
        PreparedStatement selectStmt = null;
        ResultSet rs = null;
        String select = "select key_, version_ from " + name + " where key_ = ? for update";

        try {
            conn = datasource.getConnection();
            selectStmt = conn.prepareStatement(select);
            selectStmt.setBytes(1, key);
            rs = selectStmt.executeQuery();
            boolean deletedSomething = false;
            while(rs.next()) {
                byte[] theKey = rs.getBytes("key_");
                byte[] version = rs.getBytes("version_");
                if(new VectorClock(version).compare(maxVersion) == Occured.BEFORE) {
                    delete(conn, theKey, version);
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

    public List<Versioned<byte[]>> get(byte[] key) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String select = "select version_, value_ from " + name + " where key_ = ?";
        try {
            conn = datasource.getConnection();
            stmt = conn.prepareStatement(select);
            stmt.setBytes(1, key);
            rs = stmt.executeQuery();
            List<Versioned<byte[]>> found = Lists.newArrayList();
            while(rs.next()) {
                byte[] version = rs.getBytes("version_");
                byte[] value = rs.getBytes("value_");
                found.add(new Versioned<byte[]>(value, new VectorClock(version)));
            }

            return found;
        } catch(SQLException e) {
            throw new PersistenceFailureException("Fix me!", e);
        } finally {
            tryClose(rs);
            tryClose(stmt);
            tryClose(conn);
        }
    }

    public String getName() {
        return name;
    }

    public void put(byte[] key, Versioned<byte[]> value) throws PersistenceFailureException {
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
            select.setBytes(1, key);
            results = select.executeQuery();
            while(results.next()) {
                byte[] thisKey = results.getBytes("key_");
                VectorClock version = new VectorClock(results.getBytes("version_"));
                Occured occured = value.getVersion().compare(version);
                if(occured == Occured.BEFORE)
                    throw new ObsoleteVersionException("Attempt to put version "
                                                       + value.getVersion()
                                                       + " which is superceeded by " + version
                                                       + ".");
                else if(occured == Occured.AFTER)
                    delete(conn, thisKey, version.toBytes());
            }

            // Okay, cool, now put the value
            insert = conn.prepareStatement(insertSql);
            insert.setBytes(1, key);
            VectorClock clock = (VectorClock) value.getVersion();
            insert.setBytes(2, clock.toBytes());
            insert.setBytes(3, value.getValue());
            insert.executeUpdate();
        } catch(SQLException e) {
            if(e.getErrorCode() == MYSQL_ERR_DUP_KEY || e.getErrorCode() == MYSQL_ERR_DUP_ENTRY) {
                e.printStackTrace();
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

    private class MysqlClosableIterator implements
            ClosableIterator<Entry<byte[], Versioned<byte[]>>> {

        private boolean hasMore;
        private final ResultSet rs;
        private final Connection connection;
        private final PreparedStatement statement;

        public MysqlClosableIterator(Connection connection,
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

        public void close() {
            tryClose(rs);
            tryClose(statement);
            tryClose(connection);
        }

        public boolean hasNext() {
            return this.hasMore;
        }

        public Entry<byte[], Versioned<byte[]>> next() {
            try {
                if(!this.hasMore)
                    throw new PersistenceFailureException("Next called on iterator, but no more items available!");
                byte[] key = rs.getBytes("key_");
                byte[] value = rs.getBytes("value_");
                VectorClock clock = new VectorClock(rs.getBytes("version_"));
                this.hasMore = rs.next();
                return new Entry<byte[], Versioned<byte[]>>(key,
                                                            new Versioned<byte[]>(value, clock));
            } catch(SQLException e) {
                throw new PersistenceFailureException(e);
            }
        }

        public void remove() {
            try {
                rs.deleteRow();
            } catch(SQLException e) {
                throw new PersistenceFailureException(e);
            }
        }

    }

}
