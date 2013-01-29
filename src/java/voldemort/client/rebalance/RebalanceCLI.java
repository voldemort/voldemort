/*
 * Copyright 2012-2013 LinkedIn, Inc
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

package voldemort.client.rebalance;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.utils.ClusterInstance;
import voldemort.utils.CmdUtils;
import voldemort.utils.Entropy;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceClusterUtils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;

public class RebalanceCLI {

    private final static int SUCCESS_EXIT_CODE = 0;
    private final static int ERROR_EXIT_CODE = 1;
    private final static int HELP_EXIT_CODE = 2;
    private final static Logger logger = Logger.getLogger(RebalanceCLI.class);

    private final static int DEFAULT_GENERATE_RANDOM_SWAP_ATTEMPTS = 100;
    private final static int DEFAULT_GENERATE_RANDOM_SWAP_SUCCESSES = 100;
    private final static int DEFAULT_GENERATE_GREEDY_SWAP_ATTEMPTS = 5;
    private final static int DEFAULT_GENERATE_GREEDY_MAX_PARTITIONS_PER_NODE = 5;
    private final static int DEFAULT_GENERATE_GREEDY_MAX_PARTITIONS_PER_ZONE = 25;
    private final static int DEFAULT_GENERATE_MAX_CONTIGUOUS_PARTITIONS = 0;

    public static void main(String[] args) throws Exception {
        int exitCode = ERROR_EXIT_CODE;
        RebalanceController rebalanceController = null;
        try {
            OptionParser parser = new OptionParser();
            parser.accepts("help", "Print usage information");
            parser.accepts("current-cluster", "Path to current cluster xml")
                  .withRequiredArg()
                  .describedAs("cluster.xml");
            parser.accepts("target-cluster", "Path to target cluster xml")
                  .withRequiredArg()
                  .describedAs("cluster.xml");
            parser.accepts("current-stores", "Path to store definition xml")
                  .withRequiredArg()
                  .describedAs("stores.xml");
            parser.accepts("url", "Url to bootstrap from ").withRequiredArg().describedAs("url");
            parser.accepts("parallelism",
                           "Number of rebalances to run in parallel [ Default:"
                                   + RebalanceClientConfig.MAX_PARALLEL_REBALANCING + " ]")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("parallelism");
            parser.accepts("tries",
                           "(1) Tries during rebalance [ Default: "
                                   + RebalanceClientConfig.MAX_TRIES_REBALANCING
                                   + " ] (2) Number of tries while generating new metadata")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("num-tries");
            parser.accepts("generate",
                           "Optimize the target cluster which has new nodes with empty partitions");
            parser.accepts("generate-disable-primary-balancing",
                           "Make sure that all nodes within every zone have the same (within one) number of primary partitions [default: enabled]");
            parser.accepts("generate-enable-xzone-primary-moves",
                           "Allow primary partitions to move across zones [Default: disabled]");
            parser.accepts("generate-enable-any-xzone-nary-moves",
                           "Allow non-primary partitions to move across zones at any time (i.e., does not check for xzone moves) [Default: disabled]");
            parser.accepts("generate-enable-last-resort-xzone-nary-moves",
                           "Allow non-primary partitions to move across zones as a last resort (i.e., checks for xzone moves and prefers to avoid them, unless a xzone move is required to achieve balance) [Default: disabled]");
            parser.accepts("generate-enable-xzone-shuffle",
                           "Allows non-primary partitions to move across zones in random or greedy shuffles. [Default: disabled]");
            parser.accepts("generate-enable-random-swaps",
                           "Enable attempts to improve balance by random partition swaps within a zone. [Default: disabled]");
            parser.accepts("generate-random-swap-attempts",
                           "Number of random swaps to attempt. [Default:"
                                   + DEFAULT_GENERATE_RANDOM_SWAP_ATTEMPTS + " ]")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("num-attempts");
            parser.accepts("generate-random-swap-successes",
                           "Number of successful random swaps to permit exit before completing all swap attempts. [Default:"
                                   + DEFAULT_GENERATE_RANDOM_SWAP_SUCCESSES + " ]")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("num-successes");
            parser.accepts("generate-enable-greedy-swaps",
                           "Enable attempts to improve balance by greedily swapping (random) partitions within a zone. [Default: disabled]");
            parser.accepts("generate-greedy-swap-attempts",
                           "Number of greedy (random) swaps to attempt. [Default:"
                                   + DEFAULT_GENERATE_GREEDY_SWAP_ATTEMPTS + " ]")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("num-attempts");
            parser.accepts("generate-greedy-max-partitions-per-node",
                           "Max number of partitions per-node to evaluate swapping with other partitions within the zone. [Default:"
                                   + DEFAULT_GENERATE_GREEDY_MAX_PARTITIONS_PER_NODE + " ]")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("max-partitions-per-node");
            parser.accepts("generate-greedy-max-partitions-per-zone",
                           "Max number of (random) partitions per-zone to evaluate swapping with partitions from node being evaluated. [Default:"
                                   + DEFAULT_GENERATE_GREEDY_MAX_PARTITIONS_PER_ZONE + " ]")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("max-partitions-per-zone");
            parser.accepts("generate-max-contiguous-partitions",
                           "Limit the number of contiguous partition IDs allowed within a zone. [Default:"
                                   + DEFAULT_GENERATE_MAX_CONTIGUOUS_PARTITIONS
                                   + " (indicating no limit)]")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("num-contiguous");
            parser.accepts("analyze", "Analyze how balanced given cluster is.");
            parser.accepts("entropy",
                           "True - if we want to run the entropy calculator. False - if we want to store keys")
                  .withRequiredArg()
                  .ofType(Boolean.class);
            parser.accepts("output-dir",
                           "Specify the output directory for (1) dumping metadata"
                                   + "(b) dumping entropy keys")
                  .withRequiredArg()
                  .ofType(String.class)
                  .describedAs("path");
            parser.accepts("delete",
                           "Delete after rebalancing (Valid only for RW Stores) [ Default : false ] ");
            parser.accepts("show-plan",
                           "Shows the rebalancing plan only without executing the rebalance");
            parser.accepts("keys",
                           "The number of keys to use for entropy calculation [ Default : "
                                   + Entropy.DEFAULT_NUM_KEYS + " ]")
                  .withRequiredArg()
                  .ofType(Long.class)
                  .describedAs("num-keys");
            parser.accepts("timeout",
                           "Time-out in seconds for rebalancing of a single task ( stealer - donor tuple ) [ Default : "
                                   + RebalanceClientConfig.REBALANCING_CLIENT_TIMEOUT_SEC + " ]")
                  .withRequiredArg()
                  .ofType(Long.class)
                  .describedAs("sec");
            parser.accepts("batch",
                           "Number of primary partitions to move together [ Default : "
                                   + RebalanceClientConfig.PRIMARY_PARTITION_BATCH_SIZE + " ]")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("num-primary-partitions");
            parser.accepts("stealer-based",
                           "Run the rebalancing from the stealer node's perspective [ Default : "
                                   + RebalanceClientConfig.STEALER_BASED_REBALANCING + " ]")
                  .withRequiredArg()
                  .ofType(Boolean.class)
                  .describedAs("boolean");
            parser.accepts("verbose-logging",
                           "Verbose logging such as keys found missing on specific nodes during post-rebalancing entropy verification");

            OptionSet options = parser.parse(args);

            if(options.has("help")) {
                printHelp(System.out, parser);
                System.exit(HELP_EXIT_CODE);
            }

            boolean deleteAfterRebalancing = options.has("delete");
            int parallelism = CmdUtils.valueOf(options,
                                               "parallelism",
                                               RebalanceClientConfig.MAX_PARALLEL_REBALANCING);
            int maxTriesRebalancing = CmdUtils.valueOf(options,
                                                       "tries",
                                                       RebalanceClientConfig.MAX_TRIES_REBALANCING);
            boolean enabledShowPlan = options.has("show-plan");
            long rebalancingTimeoutSeconds = CmdUtils.valueOf(options,
                                                              "timeout",
                                                              RebalanceClientConfig.REBALANCING_CLIENT_TIMEOUT_SEC);
            int primaryPartitionBatchSize = CmdUtils.valueOf(options,
                                                             "batch",
                                                             RebalanceClientConfig.PRIMARY_PARTITION_BATCH_SIZE);
            boolean stealerBasedRebalancing = CmdUtils.valueOf(options,
                                                               "stealer-based",
                                                               RebalanceClientConfig.STEALER_BASED_REBALANCING);
            boolean generateDisablePrimaryBalancing = options.has("generate-disable-primary-balancing");
            boolean generateEnableXzonePrimary = options.has("generate-enable-xzone-primary-moves");
            boolean generateEnableAllXzoneNary = options.has("generate-enable-any-xzone-nary-moves");
            boolean generateEnableLastResortXzoneNary = options.has("generate-enable-last-resort-xzone-nary-moves");
            boolean generateEnableXzoneShuffle = options.has("generate-enable-xzone-shuffle");
            boolean generateEnableRandomSwaps = options.has("generate-enable-random-swaps");
            int generateRandomSwapAttempts = CmdUtils.valueOf(options,
                                                              "generate-random-swap-attempts",
                                                              DEFAULT_GENERATE_RANDOM_SWAP_ATTEMPTS);
            int generateRandomSwapSuccesses = CmdUtils.valueOf(options,
                                                               "generate-random-swap-successes",
                                                               DEFAULT_GENERATE_RANDOM_SWAP_SUCCESSES);
            boolean generateEnableGreedySwaps = options.has("generate-enable-greedy-swaps");
            int generateGreedySwapAttempts = CmdUtils.valueOf(options,
                                                              "generate-greedy-swap-attempts",
                                                              DEFAULT_GENERATE_GREEDY_SWAP_ATTEMPTS);
            int generateGreedyMaxPartitionsPerNode = CmdUtils.valueOf(options,
                                                                      "generate-greedy-max-partitions-per-node",
                                                                      DEFAULT_GENERATE_GREEDY_MAX_PARTITIONS_PER_NODE);
            int generateGreedyMaxPartitionsPerZone = CmdUtils.valueOf(options,
                                                                      "generate-greedy-max-partitions-per-zone",
                                                                      DEFAULT_GENERATE_GREEDY_MAX_PARTITIONS_PER_ZONE);
            int generateMaxContiguousPartitionsPerZone = CmdUtils.valueOf(options,
                                                                          "generate-max-contiguous-partitions",
                                                                          DEFAULT_GENERATE_MAX_CONTIGUOUS_PARTITIONS);

            RebalanceClientConfig config = new RebalanceClientConfig();
            config.setMaxParallelRebalancing(parallelism);
            config.setDeleteAfterRebalancingEnabled(deleteAfterRebalancing);
            config.setEnableShowPlan(enabledShowPlan);
            config.setMaxTriesRebalancing(maxTriesRebalancing);
            config.setRebalancingClientTimeoutSeconds(rebalancingTimeoutSeconds);
            config.setPrimaryPartitionBatchSize(primaryPartitionBatchSize);
            config.setStealerBasedRebalancing(stealerBasedRebalancing);

            if(options.has("output-dir")) {
                config.setOutputDirectory((String) options.valueOf("output-dir"));
            }

            if(options.has("url")) {

                if(!options.has("target-cluster")) {
                    System.err.println("Missing required arguments: target-cluster");
                    printHelp(System.err, parser);
                    System.exit(ERROR_EXIT_CODE);
                }

                String targetClusterXML = (String) options.valueOf("target-cluster");
                Cluster targetCluster = new ClusterMapper().readCluster(new File(targetClusterXML));

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

                if(options.has("entropy")) {

                    if(!config.hasOutputDirectory()) {
                        System.err.println("Missing arguments output-dir");
                        printHelp(System.err, parser);
                        System.exit(ERROR_EXIT_CODE);
                    }

                    boolean entropy = (Boolean) options.valueOf("entropy");
                    boolean verbose = options.has("verbose-logging");
                    long numKeys = CmdUtils.valueOf(options, "keys", Entropy.DEFAULT_NUM_KEYS);
                    Entropy generator = new Entropy(-1, numKeys, verbose);
                    generator.generateEntropy(currentCluster,
                                              storeDefs,
                                              new File(config.getOutputDirectory()),
                                              entropy);
                    return;

                }

                if(options.has("analyze")) {
                    Pair<Double, String> analysis = new ClusterInstance(currentCluster, storeDefs).analyzeBalanceVerbose();
                    System.out.println(analysis.getSecond());
                    return;
                }

                if(!options.has("target-cluster")) {
                    System.err.println("Missing required arguments: target-cluster");
                    printHelp(System.err, parser);
                    System.exit(ERROR_EXIT_CODE);
                }

                String targetClusterXML = (String) options.valueOf("target-cluster");
                Cluster targetCluster = new ClusterMapper().readCluster(new File(targetClusterXML));

                if(options.has("generate")) {
                    if(generateDisablePrimaryBalancing && !generateEnableRandomSwaps
                       && !generateEnableGreedySwaps && generateMaxContiguousPartitionsPerZone == 0) {
                        System.err.println("Specified generate but did not enable any forms for generation (balance primary partitoins, greedy swaps, random swaps, max contiguous partitions).");
                        printHelp(System.err, parser);
                        System.exit(ERROR_EXIT_CODE);
                    }
                    if((options.has("generate-random-swap-attempts") || options.has("generate-random-swap-successes"))
                       && !generateEnableRandomSwaps) {
                        System.err.println("Provided arguments for generate random swaps but disabled the feature");
                        printHelp(System.err, parser);
                        System.exit(ERROR_EXIT_CODE);
                    }
                    if((options.has("generate-greedy-swap-attempts")
                        || options.has("generate-greedy-max-partitions-per-node") || options.has("generate-greedy-max-partitions-per-zone"))
                       && !generateEnableGreedySwaps) {
                        System.err.println("Provided arguments for generate greedy swaps but disabled the feature");
                        printHelp(System.err, parser);
                        System.exit(ERROR_EXIT_CODE);
                    }
                    if(generateEnableAllXzoneNary && generateEnableLastResortXzoneNary) {
                        System.err.println("Specified both generate-enable-any-xzone-nary-moves and generate-enable-last-resort-xzone-nary-moves. Please specify at most one of these mutually exclusive options.");
                        printHelp(System.err, parser);
                        System.exit(ERROR_EXIT_CODE);
                    }
                    if(generateDisablePrimaryBalancing
                       && (generateEnableAllXzoneNary || generateEnableLastResortXzoneNary)) {
                        System.err.println("Specified generate-disable-primary-balancing but also specified either generate-enable-any-xzone-nary-moves or generate-enable-last-resort-xzone-nary-moves which will have no effect.");
                        printHelp(System.err, parser);
                        System.exit(ERROR_EXIT_CODE);
                    }
                    if(generateEnableXzoneShuffle
                       && !(generateEnableRandomSwaps || generateEnableGreedySwaps)) {
                        System.err.println("Specified generate-enable-xzone-shuffle but did not specify one of generate-enable-random-swaps or generate-enable-greedy-swaps.");
                        printHelp(System.err, parser);
                        System.exit(ERROR_EXIT_CODE);
                    }

                    RebalanceClusterUtils.balanceTargetCluster(currentCluster,
                                                               targetCluster,
                                                               storeDefs,
                                                               config.getOutputDirectory(),
                                                               config.getMaxTriesRebalancing(),
                                                               generateDisablePrimaryBalancing,
                                                               generateEnableXzonePrimary,
                                                               generateEnableAllXzoneNary,
                                                               generateEnableLastResortXzoneNary,
                                                               generateEnableXzoneShuffle,
                                                               generateEnableRandomSwaps,
                                                               generateRandomSwapAttempts,
                                                               generateRandomSwapSuccesses,
                                                               generateEnableGreedySwaps,
                                                               generateGreedySwapAttempts,
                                                               generateGreedyMaxPartitionsPerNode,
                                                               generateGreedyMaxPartitionsPerZone,
                                                               generateMaxContiguousPartitionsPerZone);
                    return;
                }

                rebalanceController = new RebalanceController(currentCluster, config);
                rebalanceController.rebalance(currentCluster, targetCluster, storeDefs);

            }

            exitCode = SUCCESS_EXIT_CODE;
            if(logger.isInfoEnabled()) {
                logger.info("Successfully terminated rebalance all tasks");
            }

        } catch(VoldemortException e) {
            logger.error("Unsuccessfully terminated rebalance operation - " + e.getMessage(), e);
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
        stream.println();
        stream.println("REBALANCE (RUN PROCESS)");
        stream.println("a) --url <url> --target-cluster <path> [ Run the actual rebalancing process ] ");

        stream.println();
        stream.println("REBALANCE (GENERATE PLAN)");
        stream.println("b) --current-cluster <path> --current-stores <path> --target-cluster <path>");
        stream.println("\t (1) --no-delete [ Will not delete the data after rebalancing ]");
        stream.println("\t (2) --show-plan [ Will generate only the plan ]");
        stream.println("\t (3) --output-dir [ Path to output dir where we store intermediate metadata ]");
        stream.println("\t (4) --parallelism [ Number of parallel stealer - donor node tasks to run in parallel ] ");
        stream.println("\t (5) --tries [ Number of times we try to move the data before declaring failure ]");
        stream.println("\t (6) --timeout [ Timeout in seconds for one rebalancing task ( stealer - donor tuple ) ]");
        stream.println("\t (7) --batch [ Number of primary partitions to move together ]");
        stream.println("\t (8) --stealer-based [ Run the rebalancing from the stealers perspective ]");

        stream.println();
        stream.println("GENERATE");
        stream.println("a) --current-cluster <path> --current-stores <path> --target-cluster <path> --generate [ Generates a new cluster xml with least number of movements."
                       + " Uses target cluster i.e. current-cluster + new nodes ( with empty partitions ) ]");
        stream.println("\t (1)  --output-dir [ Output directory is where we store the optimized cluster ]");
        stream.println("\t (2) --tries [ Number of optimization cycles ] ");
        stream.println("\t (3) --generate-disable-primary-balancing [ Do not balance number of primary partitions across nodes within each zone ] ");
        stream.println("\t (4) --generate-enable-xzone-primary-moves [ Allow primary partitions to move across zones ] ");
        stream.println("\t (5) --generate-enable-any-xzone-nary-moves [ Allow non-primary partitions to move across zones. ]");
        stream.println("\t (6) --generate-enable-last-resort-xzone-nary-moves [ Allow non-primary partitions to move across zones as a last resort --- Will only do such a move if all possible moves result in xzone move.] ");
        stream.println("\t (7) --generate-enable-xzone-shuffle [ Allow non-primary partitions to move across zones for random swaps or greedy swaps.] ");
        stream.println("\t (8) --generate-enable-random-swaps [ Attempt to randomly swap partitions within a zone to improve balance ] ");
        stream.println("\t (9) --generate-random-swap-attempts num-attempts [ Number of random swaps to attempt in hopes of improving balance ] ");
        stream.println("\t(10) --generate-random-swap-successes num-successes [ Stop after num-successes successful random swap atttempts ] ");
        stream.println("\t(11) --generate-enable-greedy-swaps [ Attempt to greedily (randomly) swap partitions within a zone to improve balance. Greedily/randomly means sample many swaps for each node and choose best swap. ] ");
        stream.println("\t(12) --generate-greedy-swap-attempts num-attempts [ Number of greedy swap passes to attempt. Each pass can be fairly expensive. ] ");
        stream.println("\t(13) --generate-greedy-max-partitions-per-node num-partitions [ num-partitions per node to consider in each greedy pass. Partitions selected randomly from each node.  ] ");
        stream.println("\t(14) --generate-greedy-max-partitions-per-zone num-partitions [ num-partitions per zone to consider in each greedy pass. Partitions selected randomly from all partitions in zone not on node being considered. ] ");
        stream.println("\t(15) --generate-max-contiguous-partitions num-contiguous [ Max allowed contiguous partition IDs within a zone ] ");

        stream.println();
        stream.println("ANALYZE");
        stream.println("a) --current-cluster <path> --current-stores <path> --analyze [ Analyzes a cluster xml for balance]");
        stream.println();
        stream.println("ENTROPY");
        stream.println("a) --current-cluster <path> --current-stores <path> --entropy <true / false> --output-dir <path> [ Runs the entropy calculator if "
                       + "--entropy is true. Else dumps keys to the directory ]");
        stream.println("\t (1) --keys [ Number of keys ( per store ) we calculate entropy for ]");
        stream.println("\t (2) --verbose-logging [ print keys found missing during entropy ]");
        parser.printHelpOn(stream);
    }
}
