package voldemort.performance;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.AdminClientFactory;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClientRequestFormat;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

public class AdminTest {

    private final String storeName;
    private final AdminClientRequestFormat adminClient;

    public static interface Measurable {

        long apply();
    }

    public static interface Timed {

        void apply();
    }

    private static final String usageStr = "Usage: $VOLDEMORT_HOME/bin/admin-test.sh \\\n"
                                           + "\t [options] bootstrapUrl storeName";

    public AdminTest(String bootstrapUrl, String storeName) {
        this(bootstrapUrl, storeName, false);
    }

    public AdminTest(String bootstrapUrl, String storeName, boolean useNative) {
        this.storeName = storeName;
        AdminClientFactory adminClientFactory = new AdminClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl)
                                                                                         .setConnectionTimeout(10000,
                                                                                                               TimeUnit.MILLISECONDS)
                                                                                         .setSocketTimeout(100000,
                                                                                                           TimeUnit.MILLISECONDS)
                                                                                         .setSocketBufferSize(32 * 1024));
        this.adminClient = adminClientFactory.getAdminClient();
    }

    public static void printUsage(PrintStream out, OptionParser parser, String msg)
            throws IOException {
        out.println(msg);
        out.println(usageStr);
        parser.printHelpOn(out);
        System.exit(1);
    }

    public static void printUsage(PrintStream out, OptionParser parser) throws IOException {
        out.println(usageStr);
        parser.printHelpOn(out);
        System.exit(1);
    }

    private List<Integer> getNodes(int partition) {
        List<Integer> rv = new LinkedList<Integer>();
        Cluster cluster = adminClient.getMetadata().getCluster();
        for(Node node: cluster.getNodes()) {
            if(node.getPartitionIds().contains(partition))
                rv.add(node.getId());
        }

        return rv;
    }

    private List<Integer> getPartitions(int nodeId) {
        Cluster cluster = adminClient.getMetadata().getCluster();
        Node node = cluster.getNodeById(nodeId);
        return node.getPartitionIds();
    }

    public static void measureFunction(Measurable fn, int count) {
        long ops = 0;
        long start = System.currentTimeMillis();
        for(int i = 0; i < count; i++) {
            ops += fn.apply();
        }
        long totalTime = System.currentTimeMillis() - start;

        System.out.println("Throughput: " + (ops / (double) totalTime * 1000) + " ops / sec.");
        System.out.println(ops + " ops carried out.");
    }

    public static void timeFunction(Timed fn, int count) {
        long start = System.currentTimeMillis();
        for(int i = 0; i < count; i++) {
            fn.apply();
        }
        long totalTime = System.currentTimeMillis() - start;

        System.out.println("Total time: " + totalTime / 1000);
    }

    protected SetMultimap<Integer, Integer> getNodePartitions(List<?> optNodes,
                                                              List<?> optPartitions) {
        SetMultimap<Integer, Integer> nodePartitions = HashMultimap.create();

        if(optPartitions != null && optNodes != null) {
            for(Object node: optNodes) {
                for(Object partition: optPartitions)
                    nodePartitions.put((Integer) node, (Integer) partition);
            }
        } else if(optPartitions != null) {
            for(Object partition: optPartitions) {
                for(Integer node: getNodes((Integer) partition)) {
                    nodePartitions.put(node, (Integer) partition);
                }
            }
        } else if(optNodes != null) {
            for(Object node: optNodes) {
                nodePartitions.putAll((Integer) node, getPartitions((Integer) node));
            }
        } else
            throw new IllegalStateException();

        return nodePartitions;
    }

    public void testFetch(final SetMultimap<Integer, Integer> nodePartitions) {
        for(final Integer node: nodePartitions.keySet()) {
            System.out.println("Testing fetch of node " + node + " partitions "
                               + nodePartitions.get(node) + ": \n");
            measureFunction(new Measurable() {

                public long apply() {
                    long i = 0;
                    Iterator<Pair<ByteArray, Versioned<byte[]>>> result = adminClient.fetchPartitionEntries(node,
                                                                                                            storeName,
                                                                                                            new ArrayList<Integer>(nodePartitions.get(node)),
                                                                                                            null);
                    while(result.hasNext()) {
                        i++;
                        result.next();
                    }
                    return i;
                }
            },
                            1);
        }
    }

    public void testFetchAndUpdate(final SetMultimap<Integer, Integer> from,
                                   final int to,
                                   final String store) {
        for(final Integer node: from.keySet()) {
            timeFunction(new Timed() {

                public void apply() {
                    adminClient.fetchAndUpdateStreams(node,
                                                      to,
                                                      store,
                                                      new ArrayList<Integer>(from.get(node)),
                                                      null);
                }

            }, 1);
        }
    }

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();

        parser.accepts("native", "use native admin client");
        parser.accepts("f", "execute fetch operation");
        parser.accepts("fu", "fetch and update").withRequiredArg().ofType(Integer.class);
        parser.accepts("n", "node id")
              .withRequiredArg()
              .ofType(Integer.class)
              .withValuesSeparatedBy(',');
        parser.accepts("p", "partition id")
              .withRequiredArg()
              .ofType(Integer.class)
              .withValuesSeparatedBy(',');
        OptionSet options = parser.parse(args);

        List<String> nonOptions = options.nonOptionArguments();

        String bootstrapUrl = nonOptions.get(0);
        String storeName = nonOptions.get(1);

        if(!options.has("p") && !options.has("n")) {
            printUsage(System.err, parser, "One or more node and/or one or more partition has"
                                           + " to be specified");
        }

        AdminTest adminTest;
        if(options.has("native"))
            adminTest = new AdminTest(bootstrapUrl, storeName, true);
        else
            adminTest = new AdminTest(bootstrapUrl, storeName);

        SetMultimap<Integer, Integer> nodePartitions = adminTest.getNodePartitions(options.has("n") ? options.valuesOf("n")
                                                                                                   : null,
                                                                                   options.has("p") ? options.valuesOf("p")
                                                                                                   : null);

        adminTest.testFetch(nodePartitions);
    }
}
