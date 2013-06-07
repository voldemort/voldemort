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

import voldemort.client.rebalance.RebalancePlan;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.utils.CmdUtils;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;

/*
 * This tool generates a rebalance plan based on current and final xml. The plan
 * will contain steps on how to rebalance the server such that its layout
 * changes from the 'current' state to the 'final' state.
 */

public class RebalancePlanCLI {

    private final static Logger logger = Logger.getLogger(RebalancePlanCLI.class);

    private static OptionParser parser;

    private static void setupParser() {
        parser = new OptionParser();
        parser.accepts("help", "Print usage information");
        parser.accepts("current-cluster", "Path to current cluster xml")
              .withRequiredArg()
              .describedAs("cluster.xml");
        parser.accepts("final-cluster", "Path to final cluster xml")
              .withRequiredArg()
              .describedAs("cluster.xml");
        parser.accepts("current-stores",
                       "Path to current store definition xml. Needed for cluster and zone expansion.")
              .withRequiredArg()
              .describedAs("stores.xml");
        parser.accepts("final-stores",
                       "Path to final store definition xml. Needed for zone expansion.")
              .withRequiredArg()
              .describedAs("stores.xml");
        parser.accepts("batch-size",
                       "Number of primary partitions to move together [ Default : "
                               + RebalancePlan.BATCH_SIZE + " ]")
              .withRequiredArg()
              .ofType(Integer.class)
              .describedAs("num-primary-partitions");
        parser.accepts("output-dir", "Output directory in which to dump per-batch metadata")
              .withRequiredArg()
              .ofType(String.class)
              .describedAs("path");
    }

    private static void printUsage() {
        StringBuilder help = new StringBuilder();
        help.append("RebalancePlanCLI\n");
        help.append("Generates a rebalance plan based on current and final xml. The plan will "
                    + "contain steps  on how to rebalance the server such that its layout"
                    + "changes from current to final state");
        help.append("Options:\n");
        help.append("  Required:\n");
        help.append("    --current-cluster <clusterXML>\n");
        help.append("    --current-stores <storesXML>\n");
        help.append("    --final-cluster <clusterXML>\n");
        help.append("  Optional:\n");
        help.append("    --final-stores <storesXML> [ Needed for zone expansion ]\n");
        help.append("    --batch <batch> [ Number of primary partitions to move in each rebalancing batch. ]\n");
        help.append("    --output-dir <outputDir> [ Directory in which cluster metadata is dumped for each batch of the plan. ]\n");

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
                                               "current-stores",
                                               "final-cluster");
        if(missing.size() > 0) {
            printUsageAndDie("Missing required arguments: " + Joiner.on(", ").join(missing));
        }
        if(options.has("final-stores") && !options.has("final-cluster")) {
            printUsageAndDie("final-stores specified, but final-cluster not specified.");
        }

        return options;
    }

    public static void main(String[] args) throws Exception {
        setupParser();
        OptionSet options = getValidOptions(args);

        // Required args
        String currentClusterXML = (String) options.valueOf("current-cluster");
        String currentStoresXML = (String) options.valueOf("current-stores");
        String finalClusterXML = (String) options.valueOf("final-cluster");

        // Required args for some use cases
        String finalStoresXML = new String(currentStoresXML);
        if(options.has("final-stores")) {
            finalStoresXML = (String) options.valueOf("final-stores");
        }

        Cluster currentCluster = new ClusterMapper().readCluster(new File(currentClusterXML));
        List<StoreDefinition> currentStoreDefs = new StoreDefinitionsMapper().readStoreList(new File(currentStoresXML));
        Cluster finalCluster = new ClusterMapper().readCluster(new File(finalClusterXML));
        List<StoreDefinition> finalStoreDefs = new StoreDefinitionsMapper().readStoreList(new File(finalStoresXML));

        // Optional args
        int batchSize = CmdUtils.valueOf(options, "batch-size", RebalancePlan.BATCH_SIZE);

        String outputDir = null;
        if(options.has("output-dir")) {
            outputDir = (String) options.valueOf("output-dir");
        }

        new RebalancePlan(currentCluster,
                          currentStoreDefs,
                          finalCluster,
                          finalStoreDefs,
                          batchSize,
                          outputDir);
    }

}
