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

import voldemort.client.rebalance.RebalanceController;
import voldemort.client.rebalance.RebalancePlan;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.utils.CmdUtils;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;

public class RebalanceControllerCLI {

    private final static Logger logger = Logger.getLogger(RebalanceControllerCLI.class);

    private static OptionParser parser;

    private static void setupParser() {
        parser = new OptionParser();
        parser.accepts("help", "Print usage information");
        parser.accepts("url", "Url to bootstrap from ").withRequiredArg().describedAs("url");
        parser.accepts("donor-based", "Execute donor-based rebalancing.");
        parser.accepts("stealer-based", "Execute stealer-based rebalancing (default).");
        // TODO: Can this option be deprecated?
        parser.accepts("tries",
                       "Tries during stealer-based rebalance [ Default: "
                               + RebalanceController.MAX_TRIES_REBALANCING + " ]")
              .withRequiredArg()
              .ofType(Integer.class)
              .describedAs("num-tries");
        parser.accepts("parallelism",
                       "Number of servers running stealer- or donor-based tasks in parallel [ Default:"
                               + RebalanceController.MAX_PARALLEL_REBALANCING + " ]")
              .withRequiredArg()
              .ofType(Integer.class)
              .describedAs("parallelism");
        parser.accepts("final-cluster", "Path to target cluster xml")
              .withRequiredArg()
              .describedAs("cluster.xml");
        parser.accepts("final-stores",
                       "Path to target store definition xml. Needed for zone expansion.")
              .withRequiredArg()
              .describedAs("stores.xml");

        parser.accepts("batch-size",
                       "Number of primary partitions to move together [ RebalancePlan parameter; Default : "
                               + RebalancePlan.BATCH_SIZE + " ]")
              .withRequiredArg()
              .ofType(Integer.class)
              .describedAs("num-primary-partitions");
        parser.accepts("output-dir",
                       "RebalancePlan parameter; Output directory in which to dump per-batch metadata")
              .withRequiredArg()
              .ofType(String.class)
              .describedAs("path");
    }

    private static void printUsage() {
        StringBuilder help = new StringBuilder();
        help.append("RebalanceControllerCLI\n");
        help.append("  Executes a rebalance plan.\n");
        help.append("Options:\n");
        help.append("  Required:\n");
        help.append("    --url <BootstrapURL>\n");
        help.append("    --final-cluster <clusterXML>\n");
        help.append("  Optional:\n");
        help.append("    --final-stores <storesXML> [ Needed for zone expansion ]\n");
        help.append("    --parallelism <parallelism> [ Number of rebalancing tasks to run in parallel ]");
        help.append("    --tries <tries> [ Number of times to try starting an async rebalancing task on a node ");
        help.append("    --output-dir [ Output directory in which plan is stored ]\n");
        help.append("    --batch <batch> [ Number of primary partitions to move in each rebalancing batch. ]\n");
        help.append("    --output-dir <outputDir> [ Directory in which cluster metadata is dumped for each batch of the plan. ]\n");
        help.append("    --stealer-based or --donor-based [ Defaults to stealer-based. ]\n");

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

        Set<String> missing = CmdUtils.missing(options, "url", "target-cluster");
        if(missing.size() > 0) {
            printUsageAndDie("Missing required arguments: " + Joiner.on(", ").join(missing));
        }

        return options;
    }

    public static void main(String[] args) throws Exception {
        setupParser();
        OptionSet options = getValidOptions(args);

        // Bootstrap & fetch current cluster/stores
        String bootstrapURL = (String) options.valueOf("url");

        // Process optional "controller" arguments
        boolean stealerBased = true;
        if(options.has("donor-based")) {
            stealerBased = false;
        }

        int parallelism = RebalanceController.MAX_PARALLEL_REBALANCING;
        if(options.has("parallelism")) {
            parallelism = (Integer) options.valueOf("parallelism");
        }

        int tries = RebalanceController.MAX_TRIES_REBALANCING;
        if(options.has("tries")) {
            tries = (Integer) options.valueOf("tries");
        }

        RebalanceController rebalanceController = new RebalanceController(bootstrapURL,
                                                                          parallelism,
                                                                          tries,
                                                                          stealerBased);

        Cluster currentCluster = rebalanceController.getCurrentCluster();
        List<StoreDefinition> currentStoreDefs = rebalanceController.getCurrentStoreDefs();
        // If this test doesn't pass, something is wrong in prod!
        RebalanceUtils.validateClusterStores(currentCluster, currentStoreDefs);

        // Determine final cluster/stores and validate them
        String finalClusterXML = (String) options.valueOf("final-cluster");
        Cluster finalCluster = new ClusterMapper().readCluster(new File(finalClusterXML));

        List<StoreDefinition> finalStoreDefs = currentStoreDefs;
        if(options.has("final-stores")) {
            String storesXML = (String) options.valueOf("final-stores");
            finalStoreDefs = new StoreDefinitionsMapper().readStoreList(new File(storesXML));
        }
        RebalanceUtils.validateClusterStores(finalCluster, finalStoreDefs);
        RebalanceUtils.validateCurrentFinalCluster(currentCluster, finalCluster);

        // Process optional "planning" arguments
        int batchSize = CmdUtils.valueOf(options, "batch-size", RebalancePlan.BATCH_SIZE);

        String outputDir = null;
        if(options.has("output-dir")) {
            outputDir = (String) options.valueOf("output-dir");
        }

        // Plan & execute rebalancing.
        rebalanceController.rebalance(new RebalancePlan(currentCluster,
                                                        currentStoreDefs,
                                                        finalCluster,
                                                        finalStoreDefs,
                                                        batchSize,
                                                        outputDir));
    }
}
