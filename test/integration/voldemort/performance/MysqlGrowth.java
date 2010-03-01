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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.dbcp.BasicDataSource;

/**
 * create table test_table (key_ varchar(200) primary key, value_ varchar(200))
 * engine=InnoDB;
 * 
 * 
 */
public class MysqlGrowth {

    public static void main(String[] args) throws Exception {
        if(args.length != 3) {
            System.err.println("USAGE: java MySQLGrowth total_size increment threads");
            System.exit(1);
        }
        final int totalSize = Integer.parseInt(args[0]);
        final int increment = Integer.parseInt(args[1]);
        final int threads = Integer.parseInt(args[2]);

        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUsername("root");
        ds.setPassword("");
        ds.setUrl("jdbc:mysql://127.0.0.1:3306/test");
        final Connection conn = ds.getConnection();
        conn.createStatement().execute("truncate table test_table");

        final Random rand = new Random();
        int iterations = totalSize / increment;
        long[] readTimes = new long[iterations];
        long[] writeTimes = new long[iterations];
        ExecutorService service = Executors.newFixedThreadPool(threads);
        for(int i = 0; i < iterations; i++) {
            System.out.println("Starting iteration " + i);
            List<Future<Object>> results = new ArrayList<Future<Object>>(increment);
            long startTime = System.currentTimeMillis();
            final int fi = i;
            for(int j = 0; j < increment; j++) {
                final int fj = j;
                results.add(service.submit(new Callable<Object>() {

                    public Object call() throws Exception {
                        upsert(conn,
                               Integer.toString(fi * increment + fj),
                               Integer.toString(fi * increment + fj));
                        return null;
                    }
                }));
            }
            for(int j = 0; j < increment; j++)
                results.get(j).get();
            writeTimes[i] = System.currentTimeMillis() - startTime;
            System.out.println("write: " + (writeTimes[i] / (double) increment));
            results.clear();

            startTime = System.currentTimeMillis();
            for(int j = 0; j < increment; j++) {
                results.add(service.submit(new Callable<Object>() {

                    public Object call() throws Exception {
                        return select(conn, Integer.toString(rand.nextInt((fi + 1) * increment)));
                    }
                }));
            }
            for(int j = 0; j < increment; j++)
                results.get(j).get();
            readTimes[i] = (System.currentTimeMillis() - startTime);
            System.out.println("read: " + (readTimes[i] / (double) increment));
        }
        conn.close();

        System.out.println();
        System.out.println("iteration read write:");
        for(int i = 0; i < iterations; i++) {
            System.out.print(i);
            System.out.print(" " + readTimes[i] / (double) increment);
            System.out.println(" " + writeTimes[i] / (double) increment);
        }

        System.exit(0);
    }

    private static void upsert(Connection conn, String key, String value) throws Exception {
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
        }
    }

    private static String select(Connection conn, String key) throws Exception {
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
        }
    }

}
