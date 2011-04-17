package voldemort.client.rebalance;

import java.io.File;
import java.util.Set;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.utils.CmdUtils;
import voldemort.xml.ClusterMapper;

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
            parser.accepts("help", "print usage information");
            parser.accepts("url", "[REQUIRED] bootstrap url")
                  .withRequiredArg()
                  .describedAs("boostrap-url");
            parser.accepts("cluster", "[REQUIRED] path to target cluster xml config file.")
                  .withRequiredArg()
                  .describedAs("target-cluster.xml");
            parser.accepts("parallelism",
                           "number of rebalances to run in parallel. (default:"
                                   + RebalanceClientConfig.MAX_PARALLEL_REBALANCING + ")")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("parallelism");
            parser.accepts("parallel-donors",
                           "number of parallel donors to run in parallel. (default:"
                                   + RebalanceClientConfig.MAX_PARALLEL_DONOR + ")")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("parallel-donors");
            parser.accepts("no-delete",
                           "Do not delete after rebalancing (Valid only for RW Stores)");
            parser.accepts("show-plan",
                           "Shows the rebalancing plan only without executing the rebalance.");

            OptionSet options = parser.parse(args);

            if(options.has("help")) {
                parser.printHelpOn(System.out);
                System.exit(HELP_EXIT_CODE);
            }

            Set<String> missing = CmdUtils.missing(options, "cluster", "url");
            if(missing.size() > 0) {
                System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
                parser.printHelpOn(System.err);
                System.exit(ERROR_EXIT_CODE);
            }

            String bootstrapURL = (String) options.valueOf("url");
            String targetClusterXML = (String) options.valueOf("cluster");
            boolean deleteAfterRebalancing = !options.has("no-delete");
            int maxParallelRebalancing = CmdUtils.valueOf(options,
                                                          "parallelism",
                                                          RebalanceClientConfig.MAX_PARALLEL_REBALANCING);
            int maxParallelDonors = CmdUtils.valueOf(options,
                                                     "parallel-donors",
                                                     RebalanceClientConfig.MAX_PARALLEL_DONOR);
            boolean enabledShowPlan = options.has("show-plan");

            Cluster targetCluster = new ClusterMapper().readCluster(new File(targetClusterXML));

            RebalanceClientConfig config = new RebalanceClientConfig();
            config.setMaxParallelRebalancing(maxParallelRebalancing);
            config.setDeleteAfterRebalancingEnabled(deleteAfterRebalancing);
            config.setMaxParallelDonors(maxParallelDonors);
            config.setEnableShowPlan(enabledShowPlan);

            rebalanceController = new RebalanceController(bootstrapURL, config);

            rebalanceController.rebalance(targetCluster);
            exitCode = SUCCESS_EXIT_CODE;
            if(logger.isInfoEnabled()) {
                logger.info("Successfully terminated rebalance");
            }

        } catch(VoldemortException e) {
            logger.error("Unsuccessfully terminated rebalance - " + e.getMessage(), e);
        } catch(OptionException e) {
            logger.error("Problem detected while parsing command line argument, please check and retry - "
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
}
