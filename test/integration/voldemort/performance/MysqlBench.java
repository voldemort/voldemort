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

package voldemort.performance;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;

/**
 * A simple MySQL benchmark
 * 
 * @author jay
 * 
 */
public class MysqlBench {

    private final ExecutorService threadPool;
    private final DataSource dataSource;
    private final int numRequests;
    private final int numThreads;

    private static void croak(String message) {
        System.err.println(message);
        System.err.println("USAGE: java MysqlBench jdbc-url db-user db-password num-requests num-threads");
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        if(args.length != 5)
            croak("Invalid number of command line arguments: expected 5 but got " + args.length
                  + ".");
        String jdbcUrl = args[0];
        String user = args[1];
        String password = args[2];
        int numRequests = Integer.parseInt(args[3]);
        int numThreads = Integer.parseInt(args[4]);
        MysqlBench bench = new MysqlBench(numThreads, numRequests, jdbcUrl, user, password);
        bench.benchmark();
    }

    public MysqlBench(int numThreads,
                      int numRequests,
                      String connectionString,
                      String username,
                      String password) {
        this.numThreads = numThreads;
        this.threadPool = Executors.newFixedThreadPool(numThreads);
        this.numRequests = numRequests;
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUsername(username);
        ds.setPassword(password);
        ds.setUrl(connectionString);
        this.dataSource = ds;
    }

    private void upsert(String key, String value) throws Exception {
        Connection conn = dataSource.getConnection();
        String upsert = "insert into test_table (key_, value_) values (?, ?) on duplicate key update value_ = ?";
        PreparedStatement stmt = conn.prepareStatement(upsert);
        try {
            stmt.setString(1, key);
            stmt.setString(2, value);
            stmt.setString(3, value);
            stmt.executeUpdate();
        } finally {
            try {
                stmt.close();
            } catch(Exception e) {}
            try {
                conn.close();
            } catch(Exception e) {}
        }
    }

    private void deleteAll() throws Exception {
        Connection conn = dataSource.getConnection();
        String delete = "delete from test_table";
        PreparedStatement stmt = conn.prepareStatement(delete);
        try {
            stmt.executeUpdate();
        } finally {
            try {
                stmt.close();
            } catch(Exception e) {}
            try {
                conn.close();
            } catch(Exception e) {}
        }
    }

    private String select(String key) throws Exception {
        Connection conn = dataSource.getConnection();
        String upsert = "select value_ from test_table where key_ = ?";
        PreparedStatement stmt = conn.prepareStatement(upsert);
        ResultSet results = null;
        try {
            stmt.setString(1, key);
            results = stmt.executeQuery();
            if(results.next())
                return results.getString(1);
            else
                return null;
        } finally {
            try {
                stmt.close();
            } catch(Exception e) {}
            try {
                conn.close();
            } catch(Exception e) {}
        }
    }

    public void benchmark() throws Exception {
        System.out.println("WRITE TEST");
        PerformanceTest writeTest = new PerformanceTest() {

            @Override
            public void doOperation(int index) throws Exception {
                upsert(Integer.toString(index), Integer.toString(index));
            }
        };
        writeTest.run(numRequests, numThreads);
        writeTest.printStats();

        System.out.println();

        System.out.println("READ TEST");
        PerformanceTest readTest = new PerformanceTest() {

            @Override
            public void doOperation(int index) throws Exception {
                select(Integer.toString(index));
            }
        };
        readTest.run(numRequests, numThreads);
        readTest.printStats();

        deleteAll();
    }

}