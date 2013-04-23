/*
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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import voldemort.client.rebalance.RebalanceClientConfig;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.utils.CmdUtils;
import voldemort.utils.RebalanceClusterUtils;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;

public class RepartitionCLI {

    private final static Logger logger = Logger.getLogger(RepartitionCLI.class);

    private final static int DEFAULT_GENERATE_RANDOM_SWAP_ATTEMPTS = 100;
    private final static int DEFAULT_GENERATE_RANDOM_SWAP_SUCCESSES = 100;
    private final static int DEFAULT_GENERATE_GREEDY_SWAP_ATTEMPTS = 5;
    private final static int DEFAULT_GENERATE_GREEDY_MAX_PARTITIONS_PER_NODE = 5;
    private final static int DEFAULT_GENERATE_GREEDY_MAX_PARTITIONS_PER_ZONE = 25;
    private final static int DEFAULT_GENERATE_MAX_CONTIGUOUS_PARTITIONS = 0;

    private static OptionParser parser;

    private static void setupParser() {
        parser = new OptionParser();
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
        parser.accepts("tries",
                       "(1) Tries during rebalance [ Default: "
                               + RebalanceClientConfig.MAX_TRIES_REBALANCING
                               + " ] (2) Number of tries while generating new metadata")
              .withRequiredArg()
              .ofType(Integer.class)
              .describedAs("num-tries");
        parser.accepts("output-dir",
                       "Specify the output directory for the repartitioned cluster.xml and the analysis files.")
              .withRequiredArg()
              .ofType(String.class)
              .describedAs("path");
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
    }

    private static void printUsage() {
        StringBuilder help = new StringBuilder();
        help.append("RepartitionCLI\n");
        // TODO: Fix this
        help.append("  Given the current cluster and target cluster, moves primary partitions among nodes"
                    + " to balance capacity and load evenly among all nodes.\n");
        help.append("Options:\n");
        help.append("  Required:\n");
        help.append("    --current-cluster <clusterXML>\n");
        help.append("    --current-stores <storesXML>\n");
        help.append("    --target-cluster <clusterXML>\n");
        // TODO: Add target-stores.
        help.append("  Optional:\n");
        help.append("    --output-dir [ Output directory is where we store the optimized cluster ]\n");
        help.append("    --tries [ Number of optimization cycles ]\n ");
        // TODO: Add rename to get rid of 'generate' prefix.
        help.append("    --generate-disable-primary-balancing [ Do not balance number of primary partitions across nodes within each zone ] \n");
        help.append("    --generate-enable-xzone-primary-moves [ Allow primary partitions to move across zones ] \n");
        help.append("    --generate-enable-any-xzone-nary-moves [ Allow non-primary partitions to move across zones. ]\n");
        help.append("    --generate-enable-last-resort-xzone-nary-moves [ Allow non-primary partitions to move across zones as a last resort --- Will only do such a move if all possible moves result in xzone move.] \n");
        help.append("    --generate-enable-xzone-shuffle [ Allow non-primary partitions to move across zones for random swaps or greedy swaps.] \n");
        help.append("    --generate-enable-random-swaps [ Attempt to randomly swap partitions within a zone to improve balance ] \n");
        help.append("    --generate-random-swap-attempts num-attempts [ Number of random swaps to attempt in hopes of improving balance ] \n");
        help.append("    --generate-random-swap-successes num-successes [ Stop after num-successes successful random swap atttempts ] \n");
        help.append("    --generate-enable-greedy-swaps [ Attempt to greedily (randomly) swap partitions within a zone to improve balance. Greedily/randomly means sample many swaps for each node and choose best swap. ] \n");
        help.append("    --generate-greedy-swap-attempts num-attempts [ Number of greedy swap passes to attempt. Each pass can be fairly expensive. ] \n");
        help.append("    --generate-greedy-max-partitions-per-node num-partitions [ num-partitions per node to consider in each greedy pass. Partitions selected randomly from each node.  ] \n");
        help.append("    --generate-greedy-max-partitions-per-zone num-partitions [ num-partitions per zone to consider in each greedy pass. Partitions selected randomly from all partitions in zone not on node being considered. ] \n");
        help.append("    --generate-max-contiguous-partitions num-contiguous [ Max allowed contiguous partition IDs within a zone ] \n");

        try {
            parser.printHelpOn(System.out);
        } catch(IOException e) {
            e.printStackTrace();
        }
        System.out.print(help.toString());
    }

    private static void printUsageAndDie(String errMessage) {
        printUsage();
        Utils.croak("\n" + errMessage);
    }

    private static OptionSet getValidOptions(String[] args) {
        OptionSet options = null;
        try {
            options = parser.parse(args);
        } catch(OptionException oe) {
            printUsageAndDie("Exception when parsing arguments : " + oe.getMessage());
        }

        if(options.has("help")) {
            printUsage();
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options,
                                               "current-cluster",
                                               "target-cluster",
                                               "current-stores");
        if(missing.size() > 0) {
            printUsageAndDie("Missing required arguments: " + Joiner.on(", ").join(missing));
        }

        return options;
    }

    // TODO: Clean up argument parsing and option handling.
    public static void main(String[] args) throws Exception {
        setupParser();
        OptionSet options = getValidOptions(args);

        int maxTriesRebalancing = CmdUtils.valueOf(options,
                                                   "tries",
                                                   RebalanceClientConfig.MAX_TRIES_REBALANCING);
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
        config.setMaxTriesRebalancing(maxTriesRebalancing);
        if(options.has("output-dir")) {
            config.setOutputDirectory((String) options.valueOf("output-dir"));
        }

        String currentClusterXML = (String) options.valueOf("current-cluster");
        String currentStoresXML = (String) options.valueOf("current-stores");
        String targetClusterXML = (String) options.valueOf("target-cluster");

        Cluster currentCluster = new ClusterMapper().readCluster(new File(currentClusterXML));
        List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new File(currentStoresXML));
        Cluster targetCluster = new ClusterMapper().readCluster(new File(targetClusterXML));

        if(generateDisablePrimaryBalancing && !generateEnableRandomSwaps
           && !generateEnableGreedySwaps && generateMaxContiguousPartitionsPerZone == 0) {
            printUsageAndDie("Specified generate but did not enable any forms for generation (balance primary partitoins, greedy swaps, random swaps, max contiguous partitions).");
        }
        if((options.has("generate-random-swap-attempts") || options.has("generate-random-swap-successes"))
           && !generateEnableRandomSwaps) {
            printUsageAndDie("Provided arguments for generate random swaps but disabled the feature");
        }
        if((options.has("generate-greedy-swap-attempts")
            || options.has("generate-greedy-max-partitions-per-node") || options.has("generate-greedy-max-partitions-per-zone"))
           && !generateEnableGreedySwaps) {
            printUsageAndDie("Provided arguments for generate greedy swaps but disabled the feature");
        }
        if(generateEnableAllXzoneNary && generateEnableLastResortXzoneNary) {
            printUsageAndDie("Specified both generate-enable-any-xzone-nary-moves and generate-enable-last-resort-xzone-nary-moves. Please specify at most one of these mutually exclusive options.");
        }
        if(generateDisablePrimaryBalancing
           && (generateEnableAllXzoneNary || generateEnableLastResortXzoneNary)) {
            printUsageAndDie("Specified generate-disable-primary-balancing but also specified either generate-enable-any-xzone-nary-moves or generate-enable-last-resort-xzone-nary-moves which will have no effect.");
        }
        if(generateEnableXzoneShuffle && !(generateEnableRandomSwaps || generateEnableGreedySwaps)) {
            printUsageAndDie("Specified generate-enable-xzone-shuffle but did not specify one of generate-enable-random-swaps or generate-enable-greedy-swaps.");
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

    }

}
