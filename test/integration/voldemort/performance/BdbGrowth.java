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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * @author jay
 * 
 */
public class BdbGrowth {

    public static void main(String[] args) throws Exception {
        if(args.length != 5) {
            System.err.println("USAGE: java BdbGrowth directory cache_size total_size increment threads");
            System.exit(1);
        }

        final String dir = args[0];
        final long cacheSize = Long.parseLong(args[1]);
        final int totalSize = Integer.parseInt(args[2]);
        final int increment = Integer.parseInt(args[3]);
        final int threads = Integer.parseInt(args[4]);

        Environment environment;
        EnvironmentConfig environmentConfig;
        DatabaseConfig databaseConfig;

        environmentConfig = new EnvironmentConfig();
        environmentConfig.setCacheSize(cacheSize);
        environmentConfig.setTxnNoSync(true);
        environmentConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "1000000000");
        environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_MAX_BATCH_FILES, "100");
        environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_READ_SIZE, "52428800");
        environmentConfig.setAllowCreate(true);
        environmentConfig.setTransactional(true);
        databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        // databaseConfig.setDeferredWrite(true);
        databaseConfig.setTransactional(true);
        databaseConfig.setNodeMaxEntries(1024);
        File bdbDir = new File(dir);
        if(!bdbDir.exists()) {
            bdbDir.mkdir();
        } else {
            for(File f: bdbDir.listFiles())
                f.delete();
        }
        environment = new Environment(bdbDir, environmentConfig);
        final Database db = environment.openDatabase(null, "test", databaseConfig);

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
                        db.put(null,
                               new DatabaseEntry(Integer.toString(fi * increment + fj).getBytes()),
                               new DatabaseEntry(Integer.toString(fi * increment + fj).getBytes()));
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
                        int value = rand.nextInt((fi + 1) * increment);
                        return db.get(null,
                                      new DatabaseEntry(Integer.toString(value).getBytes()),
                                      new DatabaseEntry(Integer.toString(value).getBytes()),
                                      null);
                    }
                }));
            }
            for(int j = 0; j < increment; j++)
                results.get(j).get();
            readTimes[i] = (System.currentTimeMillis() - startTime);
            System.out.println("read: " + (readTimes[i] / (double) increment));

            // int cleaned = 0;
            // do {
            // cleaned += environment.cleanLog();
            // } while(cleaned > 0);
            // if(cleaned > 0)
            // System.out.println("Cleaned " + cleaned + " files.");
            // CheckpointConfig cp = new CheckpointConfig();
            // cp.setForce(true);
            // environment.checkpoint(null);
            // environment.compress();
            // environment.sync();
            System.out.println("Cleaning, Checkpointing and compression completed.");
        }

        System.out.println();
        System.out.println("iteration read write:");
        for(int i = 0; i < iterations; i++) {
            System.out.print(i);
            System.out.print(" " + readTimes[i] / (double) increment);
            System.out.println(" " + writeTimes[i] / (double) increment);
        }

        System.out.println(environment.getStats(null));

        System.exit(0);
    }

}