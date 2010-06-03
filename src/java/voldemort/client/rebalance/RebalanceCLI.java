package voldemort.client.rebalance;

import java.io.File;
import java.util.Set;

import com.google.common.base.Joiner;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.cluster.Cluster;
import voldemort.utils.CmdUtils;
import voldemort.xml.ClusterMapper;

public class RebalanceCLI {
    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print usage information");
        parser.accepts("url", "[REQUIRED] bootstrap url")
                       .withRequiredArg()
                       .describedAs("boostrap-url");
        parser.accepts("cluster", "path to target cluster xml config file.")
                       .withRequiredArg()
                       .describedAs("target-cluster.xml");
        parser.accepts("parallelism", "number of rebalances to run in parallel. Default = 1")
                       .withRequiredArg()
                       .ofType(Integer.class)
                       .describedAs("parallelism");
        parser.accepts("parallel-donors", "number of parallel donors to run in parallel. Default = parallelism.")
                       .withRequiredArg()
                       .ofType(Integer.class)
                       .describedAs("parallel-donors");

        parser.accepts("no-delete", "Do not delete after rebalancing");
        OptionSet options = parser.parse(args);

        if (options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options,
                                               "cluster",
                                               "url");
        if (missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        String bootstrapURL = (String) options.valueOf("url");
        String targetClusterXML = (String) options.valueOf("cluster");
        boolean deleteAfterRebalancing = !options.has("no-delete");
        int maxParallelRebalancing = CmdUtils.valueOf(options, "parallelism", 1);
        int maxParallelDonors = CmdUtils.valueOf(options, "parallel-donors", maxParallelRebalancing);

        Cluster targetCluster = new ClusterMapper().readCluster(new File(targetClusterXML));

        RebalanceClientConfig config = new RebalanceClientConfig();
        config.setMaxParallelRebalancing(maxParallelRebalancing);
        config.setDeleteAfterRebalancingEnabled(deleteAfterRebalancing);
        config.setMaxParallelDonors(maxParallelDonors);

        RebalanceController rebalanceController = new RebalanceController(bootstrapURL, config);
        rebalanceController.rebalance(targetCluster);
        rebalanceController.stop();
    }
}
