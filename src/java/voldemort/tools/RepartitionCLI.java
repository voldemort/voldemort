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

import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.utils.CmdUtils;
import voldemort.utils.RepartitionUtils;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;

public class RepartitionCLI {

    private final static Logger logger = Logger.getLogger(RepartitionCLI.class);

    private final static int DEFAULT_REPARTITION_ATTEMPTS = 5;
    private final static int DEFAULT_RANDOM_SWAP_ATTEMPTS = 100;
    private final static int DEFAULT_RANDOM_SWAP_SUCCESSES = 100;
    private final static int DEFAULT_GREEDY_SWAP_ATTEMPTS = 5;
    private final static int DEFAULT_GREEDY_MAX_PARTITIONS_PER_NODE = 5;
    private final static int DEFAULT_GREEDY_MAX_PARTITIONS_PER_ZONE = 25;
    private final static int DEFAULT_MAX_CONTIGUOUS_PARTITIONS = 0;

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
        parser.accepts("current-stores",
                       "Path to current store definition xml. Needed for cluster and zone expansion.")
              .withRequiredArg()
              .describedAs("stores.xml");
        parser.accepts("target-stores",
                       "Path to target store definition xml. Needed for zone expansion.")
              .withRequiredArg()
              .describedAs("stores.xml");
        parser.accepts("attempts",
                       "Number of attempts at repartitioning. [ Default: "
                               + DEFAULT_REPARTITION_ATTEMPTS + " ]")
              .withRequiredArg()
              .ofType(Integer.class)
              .describedAs("num-attempts");
        parser.accepts("output-dir",
                       "Specify the output directory for the repartitioned cluster.xml and the analysis files.")
              .withRequiredArg()
              .ofType(String.class)
              .describedAs("path");
        parser.accepts("disable-node-balancing",
                       "Make sure that all nodes within every zone have the same (within one) number of primary partitions [default: enabled]");
        parser.accepts("disable-zone-balancing",
                       "Make sure that all zones have the same (within one) number of primary partitions [default: enabled]");
        parser.accepts("enable-random-swaps",
                       "Enable attempts to improve balance by random partition swaps within a zone. [Default: disabled]");
        parser.accepts("random-swap-attempts",
                       "Number of random swaps to attempt. [Default:"
                               + DEFAULT_RANDOM_SWAP_ATTEMPTS + " ]")
              .withRequiredArg()
              .ofType(Integer.class)
              .describedAs("num-attempts");
        parser.accepts("random-swap-successes",
                       "Number of successful random swaps to permit exit before completing all swap attempts. [Default:"
                               + DEFAULT_RANDOM_SWAP_SUCCESSES + " ]")
              .withRequiredArg()
              .ofType(Integer.class)
              .describedAs("num-successes");
        parser.accepts("enable-greedy-swaps",
                       "Enable attempts to improve balance by greedily swapping (random) partitions within a zone. [Default: disabled]");
        parser.accepts("greedy-swap-attempts",
                       "Number of greedy (random) swaps to attempt. [Default:"
                               + DEFAULT_GREEDY_SWAP_ATTEMPTS + " ]")
              .withRequiredArg()
              .ofType(Integer.class)
              .describedAs("num-attempts");
        parser.accepts("greedy-max-partitions-per-node",
                       "Max number of partitions per-node to evaluate swapping with other partitions within the zone. [Default:"
                               + DEFAULT_GREEDY_MAX_PARTITIONS_PER_NODE + " ]")
              .withRequiredArg()
              .ofType(Integer.class)
              .describedAs("max-partitions-per-node");
        parser.accepts("greedy-max-partitions-per-zone",
                       "Max number of (random) partitions per-zone to evaluate swapping with partitions from node being evaluated. [Default:"
                               + DEFAULT_GREEDY_MAX_PARTITIONS_PER_ZONE + " ]")
              .withRequiredArg()
              .ofType(Integer.class)
              .describedAs("max-partitions-per-zone");
        parser.accepts("max-contiguous-partitions",
                       "Limit the number of contiguous partition IDs allowed within a zone. [Default:"
                               + DEFAULT_MAX_CONTIGUOUS_PARTITIONS + " (indicating no limit)]")
              .withRequiredArg()
              .ofType(Integer.class)
              .describedAs("num-contiguous");
    }

    private static void printUsage() {
        StringBuilder help = new StringBuilder();
        help.append("RepartitionCLI\n");
        help.append("  Moves partitions to achieve better balance. This can be done for rebalancing (improve balance among existing nodes),"
                    + " cluster expansion (adding nodes to some zones), and zone expansion (adding an entire new zone).\n");
        help.append("Options:\n");
        help.append("  Required:\n");
        help.append("    --current-cluster <clusterXML>\n");
        help.append("    --current-stores <storesXML>\n");
        help.append("  Optional:\n");
        help.append("    --target-cluster <clusterXML> [ Needed for cluster or zone expansion ]\n");
        help.append("    --target-stores <storesXML> [ Needed for zone expansion ]\n");
        help.append("    --output-dir [ Output directory is where we store the optimized cluster ]\n");
        help.append("    --attempts [ Number of distinct cycles of repartitioning ]\n");
        help.append("    --disable-node-balancing [ Do not balance number of primary partitions among nodes within each zone ] \n");
        help.append("    --disable-zone-balancing [ Do not balance number of primary partitions per zone ] \n");
        help.append("    --enable-random-swaps [ Attempt to randomly swap partitions to improve balance ] \n");
        help.append("    --random-swap-attempts num-attempts [ Number of random swaps to attempt in hopes of improving balance ] \n");
        help.append("    --random-swap-successes num-successes [ Stop after num-successes successful random swap atttempts ] \n");
        help.append("    --enable-greedy-swaps [ Attempt to greedily (randomly) swap partitions to improve balance. Greedily/randomly means sample many swaps for each node and choose best swap. ] \n");
        help.append("    --greedy-swap-attempts num-attempts [ Number of greedy swap passes to attempt. Each pass can be fairly expensive. ] \n");
        help.append("    --greedy-max-partitions-per-node num-partitions [ num-partitions per node to consider in each greedy pass. Partitions selected randomly from each node.  ] \n");
        help.append("    --greedy-max-partitions-per-zone num-partitions [ num-partitions per zone to consider in each greedy pass. Partitions selected randomly from all partitions in zone not on node being considered. ] \n");
        help.append("    --max-contiguous-partitions num-contiguous [ Max allowed contiguous partition IDs within a zone ] \n");

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

        Set<String> missing = CmdUtils.missing(options, "current-cluster", "current-stores");
        if(missing.size() > 0) {
            printUsageAndDie("Missing required arguments: " + Joiner.on(", ").join(missing));
        }

        return options;
    }

    public static void main(String[] args) throws Exception {
        setupParser();
        OptionSet options = getValidOptions(args);

        // Required args
        String currentClusterXML = (String) options.valueOf("current-cluster");
        String currentStoresXML = (String) options.valueOf("current-stores");
        String targetClusterXML = new String(currentClusterXML);
        if(options.has("target-cluster")) {
            targetClusterXML = (String) options.valueOf("target-cluster");
        }
        String targetStoresXML = new String(currentStoresXML);
        if(options.has("target-stores")) {
            targetStoresXML = (String) options.valueOf("target-stores");
        }

        Cluster currentCluster = new ClusterMapper().readCluster(new File(currentClusterXML));
        List<StoreDefinition> currentStoreDefs = new StoreDefinitionsMapper().readStoreList(new File(currentStoresXML));
        Cluster targetCluster = new ClusterMapper().readCluster(new File(targetClusterXML));
        List<StoreDefinition> targetStoreDefs = new StoreDefinitionsMapper().readStoreList(new File(targetStoresXML));

        // Optional administrivia args
        int attempts = CmdUtils.valueOf(options, "attempts", DEFAULT_REPARTITION_ATTEMPTS);
        String outputDir = null;
        if(options.has("output-dir")) {
            outputDir = (String) options.valueOf("output-dir");
        }

        // Optional repartitioning args
        boolean disableNodeBalancing = options.has("disable-node-balancing");
        boolean disableZoneBalancing = options.has("disable-zone-balancing");
        boolean enableRandomSwaps = options.has("enable-random-swaps");
        int randomSwapAttempts = CmdUtils.valueOf(options,
                                                  "random-swap-attempts",
                                                  DEFAULT_RANDOM_SWAP_ATTEMPTS);
        int randomSwapSuccesses = CmdUtils.valueOf(options,
                                                   "random-swap-successes",
                                                   DEFAULT_RANDOM_SWAP_SUCCESSES);
        boolean enableGreedySwaps = options.has("enable-greedy-swaps");
        int greedySwapAttempts = CmdUtils.valueOf(options,
                                                  "greedy-swap-attempts",
                                                  DEFAULT_GREEDY_SWAP_ATTEMPTS);
        int greedyMaxPartitionsPerNode = CmdUtils.valueOf(options,
                                                          "greedy-max-partitions-per-node",
                                                          DEFAULT_GREEDY_MAX_PARTITIONS_PER_NODE);
        int greedyMaxPartitionsPerZone = CmdUtils.valueOf(options,
                                                          "greedy-max-partitions-per-zone",
                                                          DEFAULT_GREEDY_MAX_PARTITIONS_PER_ZONE);
        int maxContiguousPartitionsPerZone = CmdUtils.valueOf(options,
                                                              "max-contiguous-partitions",
                                                              DEFAULT_MAX_CONTIGUOUS_PARTITIONS);

        // Sanity check optional repartitioning args
        if(disableNodeBalancing && !enableRandomSwaps && !enableGreedySwaps
           && maxContiguousPartitionsPerZone == 0) {
            printUsageAndDie("Did not enable any forms for repartitioning.");
        }
        if((options.has("random-swap-attempts") || options.has("random-swap-successes"))
           && !enableRandomSwaps) {
            printUsageAndDie("Provided arguments for generate random swaps but did not enable the feature");
        }
        if((options.has("greedy-swap-attempts") || options.has("greedy-max-partitions-per-node") || options.has("greedy-max-partitions-per-zone"))
           && !enableGreedySwaps) {
            printUsageAndDie("Provided arguments for generate greedy swaps but did not enable the feature");
        }

        RepartitionUtils.repartition(currentCluster,
                                              currentStoreDefs,
                                              targetCluster,
                                              targetStoreDefs,
                                              outputDir,
                                              attempts,
                                              disableNodeBalancing,
                                              disableZoneBalancing,
                                              enableRandomSwaps,
                                              randomSwapAttempts,
                                              randomSwapSuccesses,
                                              enableGreedySwaps,
                                              greedySwapAttempts,
                                              greedyMaxPartitionsPerNode,
                                              greedyMaxPartitionsPerZone,
                                              maxContiguousPartitionsPerZone);

    }
}
