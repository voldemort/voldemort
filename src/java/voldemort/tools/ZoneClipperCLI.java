package voldemort.tools;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Zone;
import voldemort.store.StoreDefinition;
import voldemort.utils.CmdUtils;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;

/*
 * This tool accepts a source cluster.xml-stores.xml pair and a zone id that
 * ought to be dropped. It then intelligently moves the partitions that are
 * hosted in the zone being removed to the other zones such that it leads to no
 * data movement.
 * 
 * The partitions are moved to nodes in the surviving zones that is zone-nry to
 * that partition in the surviving zone.
 * 
 * Finally it drops the zone and the corresponding nodes that belong to the zone
 * and writes a final-cluster.xml and final-stores.xml in the output directory.
 */

public class ZoneClipperCLI {

    private static final Logger logger = Logger.getLogger(ZoneClipperCLI.class);

    private static OptionParser parser;

    private static void setupParser() {
        parser = new OptionParser();
        parser.accepts("help", "Print usage information");
        parser.accepts("current-cluster", "Path to current cluster xml")
              .withRequiredArg()
              .describedAs("cluster.xml");
        parser.accepts("current-stores", "Path to current stores xml")
              .withRequiredArg()
              .describedAs("stores.xml");
        parser.accepts("output-dir", "Specify the output directory for the new cluster.xml")
              .withRequiredArg()
              .ofType(String.class)
              .describedAs("path");
        parser.accepts("drop-zoneid", "Zone id that you want to drop.")
              .withRequiredArg()
              .describedAs("zoneid-to-drop")
              .ofType(Integer.class);
    }

    private static void printUsage() {
        StringBuilder help = new StringBuilder();
        help.append("ZoneClipperCLI\n");
        help.append("  Drops specific zone and corresponding nodes from the cluster\n");
        help.append("Options:\n");
        help.append("  Required:\n");
        help.append("    --current-cluster <clusterXML>\n");
        help.append("    --current-stores <storesXML>\n");
        help.append("    --drop-zoneid zoneId \n");
        help.append("  Optional:\n");
        help.append("    --output-dir [ Output directory is where we store the final cluster/stores xml ]\n");
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
                                               "drop-zoneid");
        if(missing.size() > 0) {
            printUsageAndDie("Missing required arguments: " + Joiner.on(", ").join(missing));
        }
        return options;
    }

    public static void main(String[] args) throws Exception {
        setupParser();
        OptionSet options = getValidOptions(args);

        int dropZoneId = CmdUtils.valueOf(options, "drop-zoneid", Zone.UNSET_ZONE_ID);
        String outputDir = null;
        if(options.has("output-dir")) {
            outputDir = (String) options.valueOf("output-dir");
        }

        /*
         * A. Generate the clipped cluster.xml
         */
        String initialClusterXML = (String) options.valueOf("current-cluster");
        Cluster initialCluster = new ClusterMapper().readCluster(new File(initialClusterXML));

        // Create a list of current partition ids. We will use this set to
        // compare partitions ids in final cluster
        Set<Integer> originalPartitions = new HashSet<Integer>();
        for(Integer zoneId: initialCluster.getZoneIds()) {
            originalPartitions.addAll(initialCluster.getPartitionIdsInZone(zoneId));
        }

        // Get an intermediate cluster where parititions that belong to the zone
        // that is being dropped have been moved to the existing zones
        Cluster intermediateCluster = RebalanceUtils.vacateZone(initialCluster, dropZoneId);
        Cluster finalCluster = RebalanceUtils.dropZone(intermediateCluster, dropZoneId);

        // Make sure everything is fine
        if(initialCluster.getNumberOfPartitions() != finalCluster.getNumberOfPartitions()) {
            logger.error("The number of partitions in the initial and the final cluster is not equal \n");
        }

        Set<Integer> finalPartitions = new HashSet<Integer>();
        for(Integer zoneId: finalCluster.getZoneIds()) {
            finalPartitions.addAll(finalCluster.getPartitionIdsInZone(zoneId));
        }

        // Compare to original partition ids list
        if(!originalPartitions.equals(finalPartitions)) {
            logger.error("The list of partition ids in the initial and the final cluster doesn't match \n ");
        }

        // Finally write the final cluster to a xml file
        RebalanceUtils.dumpClusterToFile(outputDir,
                                         RebalanceUtils.finalClusterFileName,
                                         finalCluster);

        /*
         * B. Generate the clipped stores.xml
         */
        logger.info("Generating the adjusted stores.xml..");
        String initialStoresXML = (String) options.valueOf("current-stores");
        List<StoreDefinition> initialStoreDefs = new StoreDefinitionsMapper().readStoreList(new File(initialStoresXML));
        List<StoreDefinition> finalStoreDefs = RebalanceUtils.dropZone(initialStoreDefs, dropZoneId);
        RebalanceUtils.dumpStoreDefsToFile(outputDir,
                                           RebalanceUtils.finalStoresFileName,
                                           finalStoreDefs);
    }
}
