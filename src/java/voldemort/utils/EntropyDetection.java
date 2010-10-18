package voldemort.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.server.RequestRoutingType;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.versioning.Versioned;

import com.google.common.base.Joiner;

public class EntropyDetection {

    public static void main(String args[]) throws IOException {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("url", "[REQUIRED] bootstrap URL")
              .withRequiredArg()
              .describedAs("bootstrap-url")
              .ofType(String.class);
        parser.accepts("first-id", "[REQUIRED] node id for first node")
              .withRequiredArg()
              .describedAs("node-id")
              .ofType(Integer.class);
        parser.accepts("second-id", "[REQUIRED] node id for second node")
              .withRequiredArg()
              .describedAs("node-id")
              .ofType(Integer.class);
        parser.accepts("store-name", "[REQUIRED] name of the store")
              .withRequiredArg()
              .describedAs("store-name")
              .ofType(String.class);
        parser.accepts("skip-records", "number of records to skip [default: 0 i.e. none]")
              .withRequiredArg()
              .ofType(Integer.class);

        OptionSet options = parser.parse(args);

        if(options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options,
                                               "url",
                                               "first-id",
                                               "second-id",
                                               "store-name");
        if(missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        // compulsory params
        String url = (String) options.valueOf("url");
        Integer firstId = (Integer) options.valueOf("first-id");
        Integer secondId = (Integer) options.valueOf("second-id");
        String storeName = (String) options.valueOf("store-name");

        // optional params
        Integer skipRecords = CmdUtils.valueOf(options, "skip-records", 0);

        // Use admin client to get cluster / store definition
        AdminClient client = new AdminClient(url, new AdminClientConfig());

        List<StoreDefinition> storeDefs = client.getRemoteStoreDefList(firstId).getValue();
        StoreDefinition storeDef = null;
        for(StoreDefinition def: storeDefs) {
            if(def.getName().compareTo(storeName) == 0) {
                storeDef = def;
            }
        }

        if(storeDef == null) {
            System.err.println("Store name mentioned not found");
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        // Find partitions which are replicated over to the other node
        Cluster cluster = client.getAdminClientCluster();
        Node firstNode = cluster.getNodeById(firstId), secondNode = cluster.getNodeById(secondId);

        RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                      cluster);

        List<Integer> firstNodePartitionIds = firstNode.getPartitionIds();
        List<Integer> secondNodePartitionIds = secondNode.getPartitionIds();

        // This is list of partitions which we need to retrieve
        List<Integer> finalPartitionIds = new ArrayList<Integer>();

        for(Integer firstNodePartition: firstNodePartitionIds) {
            List<Integer> replicatingPartitionIds = strategy.getReplicatingPartitionList(firstNodePartition);

            // Check if replicating partition id is one of the partition ids
            for(Integer replicatingPartitionId: replicatingPartitionIds) {
                if(secondNodePartitionIds.contains(replicatingPartitionId)) {
                    finalPartitionIds.add(firstNodePartition);
                    break;
                }
            }
        }

        if(finalPartitionIds.size() == 0) {
            System.out.println("No partition found whose replica is on the other node");
            System.exit(0);
        }
        Iterator<Pair<ByteArray, Versioned<byte[]>>> iterator = client.fetchEntries(firstId,
                                                                                    storeName,
                                                                                    finalPartitionIds,
                                                                                    null,
                                                                                    false,
                                                                                    skipRecords);

        // Get Socket store for other node
        SocketStoreFactory storeFactory = new ClientRequestExecutorPool(10, 1000, 1000, 32 * 1024);
        final Store<ByteArray, byte[], byte[]> secondStore = storeFactory.create(storeName,
                                                                                 secondNode.getHost(),
                                                                                 secondNode.getSocketPort(),
                                                                                 RequestFormatType.VOLDEMORT_V3,
                                                                                 RequestRoutingType.NORMAL);

        long totalKeyValues = 0, totalCorrect = 0;
        while(iterator.hasNext()) {
            Pair<ByteArray, Versioned<byte[]>> entry = iterator.next();
            List<Versioned<byte[]>> otherValues = secondStore.get(entry.getFirst(), null);

            totalKeyValues++;
            for(Versioned<byte[]> value: otherValues) {
                if(ByteUtils.compare(value.getValue(), entry.getSecond().getValue()) == 0) {
                    totalCorrect++;
                    break;
                }
            }
        }
        if(totalKeyValues > 0)
            System.out.println("Percent correct = " + (double) (totalCorrect / totalKeyValues)
                               * 100);
        else
            System.out.println("Percent correct = 0");
    }
}
