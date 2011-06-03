package voldemort.client.rebalance;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.utils.CmdUtils;
import voldemort.utils.KeyDistributionGenerator;
import voldemort.utils.RebalanceUtils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;

public class RebalanceCLI {

    private final static int SUCCESS_EXIT_CODE = 0;
    private final static int ERROR_EXIT_CODE = 1;
    private final static int HELP_EXIT_CODE = 2;
    private final static Logger logger = Logger.getLogger(RebalanceCLI.class);

    public static void main(String[] args) throws Exception {
        int exitCode = ERROR_EXIT_CODE;
        RebalanceController rebalanceController = null;
        try {
            OptionParser parser = new OptionParser();
            parser.accepts("help", "Print usage information");
            parser.accepts("current-cluster", "Path to current cluster xml")
                  .withRequiredArg()
                  .describedAs("cluster.xml");
            parser.accepts("target-cluster", "[REQUIRED] Path to target cluster xml")
                  .withRequiredArg()
                  .describedAs("cluster.xml");
            parser.accepts("current-stores", "Path to store definition xml")
                  .withRequiredArg()
                  .describedAs("stores.xml");
            parser.accepts("url", "Url to bootstrap from ").withRequiredArg().describedAs("url");
            parser.accepts("parallelism",
                           "number of rebalances to run in parallel (Default:"
                                   + RebalanceClientConfig.MAX_PARALLEL_REBALANCING + ")")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("parallelism");
            parser.accepts("tries",
                           "maximum number of tries for every rebalance (Default:"
                                   + RebalanceClientConfig.MAX_TRIES + ")")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("num-tries");
            parser.accepts("optimize",
                           "Optimize the target cluster which has new nodes with empty partitions");
            parser.accepts("no-delete",
                           "Do not delete after rebalancing (Valid only for RW Stores)");
            parser.accepts("show-plan",
                           "Shows the rebalancing plan only without executing the rebalance.");

            OptionSet options = parser.parse(args);

            if(options.has("help")) {
                printHelp(System.out, parser);
                System.exit(HELP_EXIT_CODE);
            }

            if(!options.has("target-cluster")) {
                System.err.println("Missing required arguments: target-cluster");
                printHelp(System.err, parser);
                System.exit(ERROR_EXIT_CODE);
            }

            String targetClusterXML = (String) options.valueOf("target-cluster");
            Cluster targetCluster = new ClusterMapper().readCluster(new File(targetClusterXML));
            boolean deleteAfterRebalancing = !options.has("no-delete");
            int maxParallelRebalancing = CmdUtils.valueOf(options,
                                                          "parallelism",
                                                          RebalanceClientConfig.MAX_PARALLEL_REBALANCING);
            int maxTriesRebalancing = CmdUtils.valueOf(options,
                                                       "tries",
                                                       RebalanceClientConfig.MAX_TRIES);
            boolean enabledShowPlan = options.has("show-plan");

            RebalanceClientConfig config = new RebalanceClientConfig();
            config.setMaxParallelRebalancing(maxParallelRebalancing);
            config.setDeleteAfterRebalancingEnabled(deleteAfterRebalancing);
            config.setEnableShowPlan(enabledShowPlan);
            config.setMaxTriesRebalancing(maxTriesRebalancing);

            if(options.has("url")) {

                // Normal execution of rebalancing
                String bootstrapURL = (String) options.valueOf("url");
                rebalanceController = new RebalanceController(bootstrapURL, config);
                rebalanceController.rebalance(targetCluster);

            } else {

                Set<String> missing = CmdUtils.missing(options, "current-cluster", "current-stores");
                if(missing.size() > 0) {
                    System.err.println("Missing required arguments: "
                                       + Joiner.on(", ").join(missing));
                    printHelp(System.err, parser);
                    System.exit(ERROR_EXIT_CODE);
                }

                String currentClusterXML = (String) options.valueOf("current-cluster");
                String currentStoresXML = (String) options.valueOf("current-stores");

                Cluster currentCluster = new ClusterMapper().readCluster(new File(currentClusterXML));
                List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new File(currentStoresXML));

                if(options.has("optimize")) {
                    boolean doCrossZoneOptimization = false;

                    Cluster minCluster = RebalanceUtils.generateMinCluster(currentCluster,
                                                                           targetCluster,
                                                                           storeDefs,
                                                                           doCrossZoneOptimization);
                    System.out.println("Current distribution");
                    System.out.println("--------------------");
                    System.out.println(KeyDistributionGenerator.printOverallDistribution(currentCluster,
                                                                                         storeDefs));
                    System.out.println("Target distribution");
                    System.out.println("--------------------");
                    System.out.println(KeyDistributionGenerator.printOverallDistribution(minCluster,
                                                                                         storeDefs));
                    System.out.println(new ClusterMapper().writeCluster(minCluster));

                    return;
                }

                rebalanceController = new RebalanceController(currentCluster, config);
                rebalanceController.rebalance(currentCluster, targetCluster, storeDefs);

            }

            exitCode = SUCCESS_EXIT_CODE;
            if(logger.isInfoEnabled()) {
                logger.info("Successfully terminated rebalance");
            }

        } catch(VoldemortException e) {
            logger.error("Unsuccessfully terminated rebalance - " + e.getMessage(), e);
        } catch(OptionException e) {
            logger.error("Problem detected while parsing command line argument, --help for more information - "
                                 + e.getMessage(),
                         e);
        } catch(Throwable e) {
            logger.error(e.getMessage(), e);
        } finally {
            if(rebalanceController != null) {
                try {
                    rebalanceController.stop();
                } catch(Exception e) {}
            }
        }
        System.exit(exitCode);
    }

    public static void printHelp(PrintStream stream, OptionParser parser) throws IOException {
        stream.println("Commands supported");
        stream.println("------------------");
        stream.println("a) --url <url> --target-cluster <path> [ Run the actual rebalancing process ] ");
        stream.println("b) --current-cluster <path> --current-stores <path> --target-cluster <path> --show-plan [ Generates the plan ]");
        stream.println("c) --current-cluster <path> --current-stores <path> --target-cluster <path> --optimize [ Target cluster is current-cluster + new nodes ( with empty partitions ) ]");
        parser.printHelpOn(stream);
    }
}
