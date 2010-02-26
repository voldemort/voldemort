package voldemort.client.rebalance;

import com.google.common.base.Joiner;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.store.StoreDefinition;
import voldemort.utils.CmdUtils;
import voldemort.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Provides a command line interface to the {@link voldemort.client.protocol.admin.AdminClient}
 *
 * @author afeinberg
 */
public class AdminCLI {
    public static void main (String [] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("url", "[REQUIRED] bootstrap URL")
                .withRequiredArg()
                .describedAs("bootstrap-url")
                .ofType(String.class);
        parser.accepts("node", "[REQUIRED] node id")
                .withRequiredArg()
                .describedAs("node-id")
                .ofType(Integer.class);
        parser.accepts("delete-partitions", "Delete partitions")
                .withRequiredArg()
                .describedAs("partition-ids")
                .withValuesSeparatedBy(',')
                .ofType(Integer.class);
        parser.accepts("restore", "Restore from replication");
        parser.accepts("parallelism", "Parallelism")
                .withRequiredArg()
                .describedAs("parallelism")
                .ofType(Integer.class);
        OptionSet options = parser.parse(args);

        if (options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options,
                                               "url",
                                               "store",
                                               "node");
        if (missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        String url = (String) options.valueOf("url");
        Integer nodeId = (Integer) options.valueOf("node");
        Integer parallelism = CmdUtils.valueOf(options, "parallelism", 5);

        String ops = "";
        if (options.has("delete-partitions")) {
            ops += "d";
        }
        if (options.has("restore")) {
            ops += "r";
        }
        if (ops.length() < 1) {
            Utils.croak("At least one of (delete-partitions, restore) must be specified");
        }

        AdminClient adminClient = new AdminClient(url, new AdminClientConfig());

        try {
            if (ops.contains("d")) {
                if (options.has("partition-ids")) {
                    System.out.println("Starting delete-partitions");
                    @SuppressWarnings("unchecked")
                    List<Integer> partitionIdList = (List<Integer>) options.valuesOf("partition-ids");
                    List<StoreDefinition> storeDefinitionList = adminClient.getRemoteStoreDefList(nodeId).getValue();
                    List<String> storeNames = new ArrayList<String>();
                    for (StoreDefinition storeDefinition: storeDefinitionList) {
                        storeNames.add(storeDefinition.getName());
                    }
                    for (String storeName: storeNames) {
                        System.out.println("Deleting partitions " + Joiner.on(", ").join(partitionIdList) + " of " + storeName);
                        adminClient.deletePartitions(nodeId, storeName, partitionIdList, null);
                    }
                    System.out.println("Finished delete-partitions");
                } else {
                    System.err.println("Not running delete-partitions: partition-ids must be specified when delete-partitions is invoked");
                }
            }
            if (ops.contains("r")) {
                System.out.println("Starting restore");
                adminClient.restoreDataFromReplications(nodeId, parallelism);
                System.err.println("Finished restore");
            }
        } catch (Exception e) {
            e.printStackTrace();
            Utils.croak(e.getMessage());
        }
    }
}
