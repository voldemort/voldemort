/*
 * Copyright 2012 LinkedIn, Inc
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
import voldemort.utils.CmdUtils;
import voldemort.utils.Entropy;
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
                                   + RebalanceClientConfig.MAX_TRIES
                                   + " ] (2) Number of tries while generating new metadata")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("num-tries");
            parser.accepts("generate",
                           "Optimize the target cluster which has new nodes with empty partitions");
            parser.accepts("generate-primaries-within-zone",
                           "Keep primary partitions within the same zone")
                  .withRequiredArg()
                  .ofType(Boolean.class);
            parser.accepts("generate-permit-xzone-moves",
                           "Allow non-primary partitions to move across zones.")
                  .withRequiredArg()
                  .ofType(Boolean.class);
            parser.accepts("generate-random-num-partitions",
                           "Vary number of partitions per node by some amount.")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("number-partitions");
            parser.accepts("generate-enable-random-swaps",
                           "Enable attempts to improve balance by random partition swaps within a zone")
                  .withRequiredArg()
                  .ofType(Boolean.class);
            parser.accepts("generate-random-swap-attempts", "Number of random swaps to attempt.")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("num-attempts");
            parser.accepts("generate-random-swap-successes",
                           "Number of successful random swaps to permit exit before completing all swap attempts.")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("num-successes");
            parser.accepts("generate-max-contiguous-partitions",
                           "Limit the number of contiguous partition IDs allowed within a zone.")
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
                                                       RebalanceClientConfig.MAX_TRIES);
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

            boolean generatePrimariesWithinZone = CmdUtils.valueOf(options,
                                                                   "generate-primaries-within-zone",
                                                                   true);
            boolean generatePermitXZoneMoves = CmdUtils.valueOf(options,
                                                                "generate-permit-xzone-moves",
                                                                true);
            int generateRandomNumPartitions = CmdUtils.valueOf(options,
                                                               "generate-random-num-partitions",
                                                               0);
            boolean generateEnableRandomSwaps = CmdUtils.valueOf(options,
                                                                 "generate-enable-random-swaps",
                                                                 true);
            int generateRandomSwapAttempts = CmdUtils.valueOf(options,
                                                              "generate-random-swap-attempts",
                                                              100);
            int generateRandomSwapSuccesses = CmdUtils.valueOf(options,
                                                               "generate-random-swap-successes",
                                                               100);
            int generateMaxContiguousPartitionsPerZone = CmdUtils.valueOf(options,
                                                                          "generate-max-contiguous-partitions",
                                                                          -1);

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
                    RebalanceUtils.analyzeBalance(currentCluster, storeDefs, true);
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
                    if((options.has("generate-random-swap-attempts") || options.has("generate-random-swap-successes"))
                       && !generateEnableRandomSwaps) {
                        System.err.println("Provided arguments for generate random swaps but disabled the feature");
                        printHelp(System.err, parser);
                    }
                    RebalanceUtils.balanceTargetCluster(currentCluster,
                                                        targetCluster,
                                                        storeDefs,
                                                        config.getOutputDirectory(),
                                                        config.getMaxTriesRebalancing(),
                                                        generatePrimariesWithinZone,
                                                        generatePermitXZoneMoves,
                                                        generateRandomNumPartitions,
                                                        generateEnableRandomSwaps,
                                                        generateRandomSwapAttempts,
                                                        generateRandomSwapSuccesses,
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
        stream.println("REBALANCE");
        stream.println("a) --url <url> --target-cluster <path> [ Run the actual rebalancing process ] ");
        stream.println("b) --current-cluster <path> --current-stores <path> --target-cluster <path> [ Generates the plan ]");
        stream.println("\t (i) --no-delete [ Will not delete the data after rebalancing ]");
        stream.println("\t (ii) --show-plan [ Will generate only the plan ]");
        stream.println("\t (iii) --output-dir [ Path to output dir where we store intermediate metadata ]");
        stream.println("\t (iv) --parallelism [ Number of parallel stealer - donor node tasks to run in parallel ] ");
        stream.println("\t (v) --tries [ Number of times we try to move the data before declaring failure ]");
        stream.println("\t (vi) --timeout [ Timeout in seconds for one rebalancing task ( stealer - donor tuple ) ]");
        stream.println("\t (vii) --batch [ Number of primary partitions to move together ]");
        stream.println("\t (viii) --stealer-based [ Run the rebalancing from the stealers perspective ]");

        stream.println();
        stream.println("GENERATE");
        stream.println("a) --current-cluster <path> --current-stores <path> --target-cluster <path> --generate [ Generates a new cluster xml with least number of movements."
                       + " Uses target cluster i.e. current-cluster + new nodes ( with empty partitions ) ]");
        stream.println("\t (i)  --output-dir [ Output directory is where we store the optimized cluster ]");
        stream.println("\t (ii) --tries [ Number of optimization cycles ] ");
        stream.println("\t (iii) --generate-primaries-within-zone [ Keep primaries within same zone, default true ] ");
        stream.println("\t (iv) --generate-permit-xzone-moves [ Allow non-primary partitions to move across zones, default true ] ");
        stream.println("\t (v) --generate-random-num-partitions num-partitions [ Allow number of partitions per node to vary by (roughly) num-partitions, default 0 ] ");
        stream.println("\t (vi) --generate-enable-random-swaps [ Attempt to randomly swap partitions within a zone to improve balance, default true ] ");
        stream.println("\t (vii) --generate-random-swamp-attempts num-attempts [ Number of random swamps to attempt in hopes of improving balance, default 100 ] ");
        stream.println("\t (viii) --generate-random-swamp-successes num-successes [ Stop after num-successes successful random swap atttempts, default 100 ] ");
        stream.println("\t (ix) --generate-max-contiguous-partitions num-contiguous [ Max allowed contiguous partition IDs within a zone, default of -1 which is no limit ] ");

        stream.println();
        stream.println("ANALYZE");
        stream.println("a) --current-cluster <path> --current-stores <path> --analyze [ Analyzes a cluster xml for balance]");
        stream.println();
        stream.println("ENTROPY");
        stream.println("a) --current-cluster <path> --current-stores <path> --entropy <true / false> --output-dir <path> [ Runs the entropy calculator if "
                       + "--entropy is true. Else dumps keys to the directory ]");
        stream.println("\t (i) --keys [ Number of keys ( per store ) we calculate entropy for ]");
        stream.println("\t (ii) --verbose-logging [ print keys found missing during entropy ]");
        parser.printHelpOn(stream);
    }
}
