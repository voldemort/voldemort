/**
 * Copyright 2013 LinkedIn, Inc
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
package voldemort.tools;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import java.io.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Callable;

public class ExportBDBToTextDump {
    static long SPLIT_SIZE = 1000000L;
    static Integer WRITER_BUFFER_SIZE = 16777216;

    public static OptionParser getParser() {
        OptionParser parser = new OptionParser();
        parser.acceptsAll(Arrays.asList("bdb"), "Store level BDB folder")
                .withRequiredArg()
                .ofType(String.class)
                .describedAs("input-file-or-folder");
        parser.acceptsAll(Arrays.asList("o", "output"), "Output folder of text dump")
                .withRequiredArg()
                .ofType(String.class)
                .describedAs("output-folder");

        return parser;
    }

    public static void validateOptions(OptionSet options) throws IOException {
        Integer exitStatus = null;
        if(options.has("help")) {
            exitStatus = 0;
            System.out.println("This programs loads a data dump to a Voldemort Store.");
            System.out.println("Supported data dump should be a file or a folder containing files with lines of data.");
            System.out.println("Each line should be in the format of the following (all in Hex Decimal format)");
            System.out.println("\n\tKEY_BINARY VECTOR_CLOCK_BINARY VALUE_BINARY\n");
        }
        else if(!options.has("input")) {
            System.err.println("Option \"input\" is required");
            exitStatus = 1;
        }
        else if(!new File((String)options.valueOf("input")).isDirectory()) {
            System.err.println("Not a directory: " + options.valueOf("input") );
            exitStatus = 1;
        }
        else if(!options.has("output")) {
            System.err.println("Option \"output\" is required");
            exitStatus = 1;
        }
        else if(!new File((String)options.valueOf("output")).isDirectory()) {
            System.err.println("Not a directory: " + options.valueOf("output") );
            exitStatus = 1;
        }
        if(exitStatus != null) {
            if(exitStatus == 0)
                getParser().printHelpOn(System.out);
            else
                getParser().printHelpOn(System.err);
            System.exit(exitStatus);
        }
    }
    public static void main(String[] argv) throws Exception {
        OptionParser parser = getParser();
        OptionSet options = parser.parse(argv);
        validateOptions(options);

        // bdb_folder output_folder
        String storeBdbFolderPath = (String) options.valueOf("bdb");
        String outputFolderPath = (String) options.valueOf("output");

        File storeBdbFolder = new File(storeBdbFolderPath);
        File outputFolder = new File(outputFolderPath);
        final String storeName = storeBdbFolder.getName();

        Properties properties = new Properties();
        properties.put("node.id","0");
        properties.put("voldemort.home", storeBdbFolder.getParent());
        VoldemortConfig voldemortConfig = new VoldemortConfig(properties);
        voldemortConfig.setBdbDataDirectory(storeBdbFolder.getParent());
        voldemortConfig.setEnableJmx(false);
        voldemortConfig.setBdbOneEnvPerStore(true);
        BdbStorageConfiguration bdbConfiguration = new BdbStorageConfiguration(voldemortConfig);
        class MockStoreDefinition extends StoreDefinition {
            public MockStoreDefinition() {
                super(storeName,null,null,null,null,null,null,null,0,null,0,null,0,null,null,null,null,null,null,null,null,null,null,null,null,0,null);
            }
            @Override
            public boolean hasMemoryFootprint() {
                return false;
            }
        }
        StoreDefinition storeDef = new MockStoreDefinition();
        StorageEngine<ByteArray, byte[], byte[]> engine = bdbConfiguration.getStore(storeDef, null);
        long reportIntervalMs = 10000L;
        long lastCount = 0;
        Reporter<Boolean> rp = new Reporter<Boolean>(reportIntervalMs);

        long count = 0;
        BufferedWriter splitFileWriter = null;
        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries = engine.entries();
        while(entries.hasNext()) {
            if(splitFileWriter == null) {
                long splitId = count / SPLIT_SIZE;
                File splitFile = new File(outputFolder, makeSplitFileName(splitId));
                splitFileWriter = new BufferedWriter(new FileWriter(splitFile), WRITER_BUFFER_SIZE);
            }
            Pair<ByteArray, Versioned<byte[]>> pair = entries.next();
            String line = makeLine(pair);
            splitFileWriter.write(line);

            if((count + 1) % SPLIT_SIZE == 0) {
                splitFileWriter.close();
                splitFileWriter = null;
            }
            count++;
            final Long countObject = count;
            Boolean reported = rp.tryReport(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    System.out.print(String.format("Exported %15d entries", countObject));
                    return true;
                }
            });
            if(reported != null) {
                System.out.println(String.format("; Speed: %8d/s", (count - lastCount)/ (reportIntervalMs / 1000)));
                lastCount = count;
            }
        }
        entries.close();
        if(splitFileWriter != null) {
            splitFileWriter.close();
        }
        System.out.println(String.format("Finished exporting %d entries", count));
    }

    static private class Reporter<T> {
        Long intervalMs;
        Long lastReport;
        Reporter(long intervalMs) {
            this.intervalMs = intervalMs;
        }

        public T tryReport(Callable<T> callable) throws Exception {
            if(lastReport == null) {
                lastReport = System.currentTimeMillis();
                return null;
            } else {
                if(lastReport + intervalMs < System.currentTimeMillis()) {
                    T result = callable.call();
                    lastReport = System.currentTimeMillis();
                    return result;
                }
            }
            return null;
        }
    }

    public static String makeLine(Pair<ByteArray, Versioned<byte[]>> pair) {
        Versioned<byte[]> versioned = pair.getSecond();
        byte[] keyBytes = pair.getFirst().get();
        byte[] versionBytes = ((VectorClock)versioned.getVersion()).toBytes();
        byte[] valueBytes = pair.getSecond().getValue();
        return String.format("%s %s %s\n", ByteUtils.toHexString(keyBytes),ByteUtils.toHexString(versionBytes),ByteUtils.toHexString(valueBytes));
    }

    public static String makeSplitFileName(long splitId) {
        return String.format("x%010d.split", splitId);
    }
}
