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
import voldemort.utils.CmdUtils;
import voldemort.utils.Entropy;
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
                                   + RebalanceClientConfig.MAX_TRIES_REBALANCING
                                   + " ] (2) Number of tries while generating new metadata")
                  .withRequiredArg()
                  .ofType(Integer.class)
                  .describedAs("num-tries");
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

                // TODO: Remove this option.
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

                if(!options.has("target-cluster")) {
                    System.err.println("Missing required arguments: target-cluster");
                    printHelp(System.err, parser);
                    System.exit(ERROR_EXIT_CODE);
                }

                String targetClusterXML = (String) options.valueOf("target-cluster");
                Cluster targetCluster = new ClusterMapper().readCluster(new File(targetClusterXML));

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
        stream.println("\t (1) --delete [ Will delete the data after rebalancing ]");
        stream.println("\t (2) --show-plan [ Will generate only the plan ]");
        stream.println("\t (3) --output-dir [ Path to output dir where we store intermediate metadata ]");
        stream.println("\t (4) --parallelism [ Number of parallel stealer - donor node tasks to run in parallel ] ");
        stream.println("\t (5) --tries [ Number of times we try to move the data before declaring failure ]");
        stream.println("\t (6) --timeout [ Timeout in seconds for one rebalancing task ( stealer - donor tuple ) ]");
        stream.println("\t (7) --batch [ Number of primary partitions to move together ]");
        stream.println("\t (8) --stealer-based false [ Run the rebalancing from the donor's perspective ]");

        stream.println();
        stream.println("ENTROPY");
        stream.println("a) --current-cluster <path> --current-stores <path> --entropy <true / false> --output-dir <path> [ Runs the entropy calculator if "
                       + "--entropy is true. Else dumps keys to the directory ]");
        stream.println("\t (1) --keys [ Number of keys ( per store ) we calculate entropy for ]");
        stream.println("\t (2) --verbose-logging [ print keys found missing during entropy ]");
        parser.printHelpOn(stream);
    }
}
