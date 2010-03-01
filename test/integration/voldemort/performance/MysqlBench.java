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

import static java.util.Arrays.asList;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Set;

import javax.sql.DataSource;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.dbcp.BasicDataSource;

import voldemort.utils.CmdUtils;
import voldemort.utils.Utils;

import com.google.common.base.Joiner;

/**
 * A simple MySQL benchmark
 * 
 * 
 */
public class MysqlBench {

    private final DataSource dataSource;
    private final String table;
    private final String requestFile;
    private final int numRequests;
    private final int numThreads;
    private final boolean doReads;
    private final boolean doWrites;

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print usage information");
        parser.acceptsAll(asList("r", "reads"), "Enable reads.");
        parser.acceptsAll(asList("w", "writes"), "Enable writes.");
        parser.acceptsAll(asList("d", "deletes"), "Enable deletes.");
        parser.accepts("table", "Table name").withRequiredArg();
        parser.accepts("db", "Database name").withRequiredArg();
        parser.acceptsAll(asList("u", "user"), "DB username.").withRequiredArg();
        parser.acceptsAll(asList("P", "password"), "DB password").withRequiredArg();
        parser.acceptsAll(asList("p", "port"), "DB port").withRequiredArg();
        parser.acceptsAll(asList("h", "host"), "DB host").withRequiredArg();
        parser.accepts("requests").withRequiredArg().ofType(Integer.class);
        parser.accepts("request-file").withRequiredArg();
        parser.accepts("threads").withRequiredArg().ofType(Integer.class);
        OptionSet options = parser.parse(args);

        if(options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options, "table", "requests", "db");
        if(missing.size() > 0)
            Utils.croak("Missing required arguments: " + Joiner.on(", ").join(missing));

        String host = CmdUtils.valueOf(options, "host", "localhost");
        String table = (String) options.valueOf("table");
        int port = CmdUtils.valueOf(options, "port", 3306);
        String database = (String) options.valueOf("db");
        String jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + database;
        String user = CmdUtils.valueOf(options, "user", "root");
        String password = CmdUtils.valueOf(options, "password", "");
        String requestFile = (String) options.valueOf("request-file");
        int numRequests = (Integer) options.valueOf("requests");
        int numThreads = CmdUtils.valueOf(options, "threads", 10);
        boolean doReads = false;
        boolean doWrites = false;
        if(options.has("reads") || options.has("writes")) {
            doReads = options.has("reads");
            doWrites = options.has("writes");
        } else {
            doReads = true;
            doWrites = true;
        }
        MysqlBench bench = new MysqlBench(table,
                                          numThreads,
                                          numRequests,
                                          jdbcUrl,
                                          user,
                                          password,
                                          requestFile,
                                          doReads,
                                          doWrites);
        bench.benchmark();
    }

    public MysqlBench(String table,
                      int numThreads,
                      int numRequests,
                      String connectionString,
                      String username,
                      String password,
                      String requestFile,
                      boolean doReads,
                      boolean doWrites) {
        this.table = table;
        this.numThreads = numThreads;
        this.numRequests = numRequests;
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUsername(username);
        ds.setPassword(password);
        ds.setUrl(connectionString);
        this.requestFile = requestFile;
        this.dataSource = ds;
        this.doReads = doReads;
        this.doWrites = doWrites;
    }

    private void upsert(String key, String value) throws Exception {
        Connection conn = dataSource.getConnection();
        String upsert = "insert into " + table
                        + " (key_, val_) values (?, ?) on duplicate key update val_ = ?";
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
        String delete = "delete from " + table;
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
        String upsert = "select val_ from " + table + " where key_ = ?";
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
        if(doWrites) {
            deleteAll();
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
        }

        if(doReads) {
            System.out.println("READ TEST");

            PerformanceTest readTest;
            if(this.requestFile == null) {
                readTest = new PerformanceTest() {

                    @Override
                    public void doOperation(int index) throws Exception {
                        select(Integer.toString(index));
                    }
                };
            } else {
                final BufferedReader reader = new BufferedReader(new FileReader(requestFile),
                                                                 1024 * 1024);
                readTest = new PerformanceTest() {

                    @Override
                    public void doOperation(int index) throws Exception {
                        select(reader.readLine().trim());
                    }
                };
            }
            readTest.run(numRequests, numThreads);
            readTest.printStats();
        }
    }
}
