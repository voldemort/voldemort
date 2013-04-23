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
import voldemort.utils.ClusterInstance;
import voldemort.utils.CmdUtils;
import voldemort.utils.PartitionBalance;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;

public class PartitionAnalysisCLI {

    private final static Logger logger = Logger.getLogger(PartitionAnalysisCLI.class);

    private static OptionParser parser;

    private static void setupParser() {
        parser = new OptionParser();
        parser.accepts("help", "Print usage information");
        parser.accepts("cluster", "Path to cluster xml")
              .withRequiredArg()
              .describedAs("cluster.xml");
        parser.accepts("stores", "Path to store definition xml")
              .withRequiredArg()
              .describedAs("stores.xml");
    }

    private static void printUsage() {
        StringBuilder help = new StringBuilder();
        help.append("PartitionAnalysisCLI\n");
        help.append("  Analyzes the partition layout of a cluster based on the storage "
                    + "definitions for that cluster. Determines how well balanced the "
                    + "partition layout is.\n");
        help.append("Options:\n");
        help.append("  Required:\n");
        help.append("    --cluster <clusterXML>\n");
        help.append("    --stores <storesXML>\n");

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

        Set<String> missing = CmdUtils.missing(options, "cluster", "stores");
        if(missing.size() > 0) {
            printUsageAndDie("Missing required arguments: " + Joiner.on(", ").join(missing));
        }

        return options;
    }

    public static void main(String[] args) throws Exception {
        setupParser();
        OptionSet options = getValidOptions(args);

        String clusterXML = (String) options.valueOf("cluster");
        String storesXML = (String) options.valueOf("stores");

        Cluster currentCluster = new ClusterMapper().readCluster(new File(clusterXML));
        List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new File(storesXML));

        PartitionBalance partitionBalance = new ClusterInstance(currentCluster, storeDefs).getPartitionBalance();
        System.out.println(partitionBalance);
    }

}
