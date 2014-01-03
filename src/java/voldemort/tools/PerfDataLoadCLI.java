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

import com.google.common.collect.AbstractIterator;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.codec.DecoderException;
import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.io.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class PerfDataLoadCLI implements Closeable {
    static Integer READER_BUFFER_SIZE = 16777216;
    final AdminClient adminClient;
    Collection<Node> targetNodeList;
    Cluster cluster;

    public PerfDataLoadCLI(String bootstrapUrl) {
        AdminClientConfig adminClientConfig = new AdminClientConfig();
        ClientConfig clientConfig = new ClientConfig();
        adminClient = new AdminClient(bootstrapUrl, adminClientConfig, clientConfig);
        cluster = adminClient.getAdminClientCluster();
        targetNodeList = cluster.getNodes();
    }

    public static OptionParser getParser() {
        OptionParser parser = new OptionParser();
        parser.acceptsAll(Arrays.asList("h","help"), "Print help message");
        parser.acceptsAll(Arrays.asList("f","input"), "Input file or containing folder of data dump")
                .withRequiredArg()
                .ofType(String.class)
                .describedAs("input-file-or-folder");
        parser.acceptsAll(Arrays.asList("s", "store"), "Store name of data to dump into")
                .withRequiredArg()
                .ofType(String.class)
                .describedAs("store-name");
        parser.acceptsAll(Arrays.asList("u", "url"), "Bootstrap URL of the destination cluster")
                .withRequiredArg()
                .ofType(String.class)
                .describedAs("bootstrap-url");
        parser.acceptsAll(Arrays.asList("node-ids", "node-id"), "Only load data to the listed node IDs(separated by comma)")
                .withRequiredArg()
                .withValuesSeparatedBy(',')
                .describedAs("node-ids")
                .ofType(Integer.class);

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
        else if(!options.has("store")) {
            System.err.println("Option \"store\" is required");
            exitStatus = 1;
        }
        else if(!options.has("url")) {
            System.err.println("Option \"url\" is required");
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

        String inputPath = (String) options.valueOf("input");
        String storeName = (String) options.valueOf("store");
        String bootstrapUrl = (String) options.valueOf("url");
        File input = new File(inputPath);
        List<File> dataFiles = new ArrayList<File>();
        if(input.isDirectory()) {
            File[] files = input.listFiles();
            if(files != null)
                Collections.addAll(dataFiles, files);
        } else if(input.isFile()) {
            dataFiles.add(input);
        } else {
            System.err.println(inputPath + "is not file or directory");
        }

        PerfDataLoadCLI cli = new PerfDataLoadCLI(bootstrapUrl);
        try {
            if (options.has("node-ids")) {
                cli.targetNodeList = new ArrayList<Node>();
                for (Integer nodeId : (List<Integer>) options.valuesOf("node-ids")) {
                    cli.targetNodeList.add(cli.cluster.getNodeById(nodeId));
                }
            }

            for(File f: dataFiles) {
                try {
                    BufferedReader bufferedReader = new BufferedReader(new FileReader(f), READER_BUFFER_SIZE);
                    cli.loadDataFromReader(bufferedReader, storeName);
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } finally {
            cli.close();
        }
        System.out.println("Completed");
    }

    protected class WrappedEntry {
        protected final Pair<ByteArray, Versioned<byte[]>> entry;

        public WrappedEntry(Pair<ByteArray, Versioned<byte[]>> entry) {
            this.entry = entry;
        }

        public Pair<ByteArray, Versioned<byte[]>> getEntry() {
            return entry;
        }
    }

    protected class UpdateEntriesIterator extends AbstractIterator<Pair<ByteArray, Versioned<byte[]>>> {
        protected final BlockingQueue<WrappedEntry> bqueue = new ArrayBlockingQueue<WrappedEntry>(1000);

        @Override
        protected Pair<ByteArray, Versioned<byte[]>> computeNext() {
            try {
                Pair<ByteArray, Versioned<byte[]>> entry = bqueue.take().getEntry();
                return (entry == null)?endOfData():entry;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return endOfData();
            }
        }
    }

    public void loadDataFromReader(final BufferedReader reader, final String storeName) throws IOException, InterruptedException {
        Cluster cluster = adminClient.getAdminClientCluster();
        List<StoreDefinition> storeDefs = adminClient.metadataMgmtOps.getRemoteStoreDefList(cluster.getNodes().iterator().next().getId()).getValue();
        StoreDefinition storeDef = null;
        for(StoreDefinition sd: storeDefs) {
            if(sd.getName() != null && sd.getName().equals(storeName)) {
                storeDef = sd;
            }
        }
        if(storeDef == null) {
            throw new VoldemortException("StoreNotfound: " + storeName);
        }
        RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef, cluster);

        Map<Integer, UpdateEntriesIterator> updateIterators = new HashMap<Integer, UpdateEntriesIterator>();
        Map<Integer, Thread> threads = new HashMap<Integer, Thread>();
        for(final Node node: targetNodeList) {
            final UpdateEntriesIterator iterator = new UpdateEntriesIterator();
            updateIterators.put(node.getId(), iterator);
            Runnable runnable = new Runnable(){
                @Override
                public void run() {
                    adminClient.streamingOps.updateEntries(node.getId(), storeName, iterator, null);
                }
            };
            String threadName = String.format("node-%02d-zone-%d-%s", node.getId(), node.getZoneId(), node.getHost());
            Thread thread = new Thread(runnable, threadName);
            threads.put(node.getId(), thread);
        }
        for(Thread t: threads.values()) {
            t.start();
        }

        try{
            while(true) {
                String line = reader.readLine();
                if(line == null) {
                    break;
                }
                Pair<ByteArray, Versioned<byte[]>> entry;
                try {
                    entry = lineToEntry(line);
                } catch (Exception e) {
                    System.err.println("Skipping line: " + line);
                    e.printStackTrace();
                    continue;
                }
                ByteArray key = entry.getFirst();
                List<Node> nodeList = routingStrategy.routeRequest(key.get());
                for(Node node: nodeList) {
                    UpdateEntriesIterator iter = updateIterators.get(node.getId());
                    if(iter != null) {
                        iter.bqueue.offer(new WrappedEntry(entry));
                    }
                }
            }
        } finally {
            for(UpdateEntriesIterator iterator: updateIterators.values()) {
                iterator.bqueue.offer(new WrappedEntry(null));  // poison pill
            }
            for(Thread t: threads.values()) {
                t.join();
            }
        }
    }

    public Pair<ByteArray, Versioned<byte[]>> lineToEntry(String line) throws DecoderException {
        String[] components = line.split(" ");

        String keyBytesString = components[0];
        byte[] keyBytes = ByteUtils.fromHexString(keyBytesString);
        ByteArray key = new ByteArray(keyBytes);

        String versionBytesString = components[1];
        byte[] versionBytes = ByteUtils.fromHexString(versionBytesString);
        Version version = new VectorClock(versionBytes, 0);

        String valueBytesString = components[1];
        byte[] value = ByteUtils.fromHexString(valueBytesString);

        return new Pair<ByteArray, Versioned<byte[]>>(key, new Versioned<byte[]>(value, version));
    }

    public void close() {
        adminClient.close();
    }
}
