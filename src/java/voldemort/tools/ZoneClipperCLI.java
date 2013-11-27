package voldemort.tools;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.utils.CmdUtils;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;

import com.google.common.base.Joiner;

/*
 * This tool accepts a spurce cluster.xml and a zone id that ought to be
 * dropped. It then intelligently moves the partitions that are hosted in the
 * zone being removed to the other zones such that it leads to no data movement.
 * 
 * The partitions are moved to nodes in the surviving zones that is zone-nry to
 * that partition in the surviving zone.
 * 
 * Finally it drops the zone and the corresponding nodes that belong to the zone
 * and writes a final-cluster.xml in the output directory.
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
        help.append("    --drop-zoneid zoneId \n");
        help.append("  Optional:\n");
        help.append("    --output-dir [ Output directory is where we store the final cluster ]\n");
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

        Set<String> missing = CmdUtils.missing(options, "current-cluster", "drop-zoneid");
        if(missing.size() > 0) {
            printUsageAndDie("Missing required arguments: " + Joiner.on(", ").join(missing));
        }
        return options;
    }

    public static void main(String[] args) throws Exception {
        setupParser();
        OptionSet options = getValidOptions(args);

        String initialClusterXML = (String) options.valueOf("current-cluster");
        Cluster initialCluster = new ClusterMapper().readCluster(new File(initialClusterXML));

        int dropZoneId = CmdUtils.valueOf(options, "drop-zoneid", Zone.UNSET_ZONE_ID);
        String outputDir = null;
        if(options.has("output-dir")) {
            outputDir = (String) options.valueOf("output-dir");
        }

        // Create a list of current partition ids. We will use this set to
        // compare partitions ids in final cluster
        Set<Integer> originalPartitions = new HashSet<Integer>();
        for(Integer zoneId: initialCluster.getZoneIds()) {
            originalPartitions.addAll(initialCluster.getPartitionIdsInZone(zoneId));
        }

        // Get an intermediate cluster where parititions that belong to the zone
        // that is being dropped have been moved to the existing zones
        Cluster intermediateCluster = RebalanceUtils.dropZone(initialCluster, dropZoneId);

        // Filter out nodes that don't belong to the zone being dropped
        Set<Node> nodes = new HashSet<Node>();
        for(int nodeId: intermediateCluster.getNodeIds()) {
            if(intermediateCluster.getNodeById(nodeId).getZoneId() != dropZoneId) {
                nodes.add(intermediateCluster.getNodeById(nodeId));
            }
        }

        // Filter out dropZoneId from all zones
        Set<Zone> zones = new HashSet<Zone>();
        for(int zoneId: intermediateCluster.getZoneIds()) {
            if(zoneId == dropZoneId) {
                continue;
            }
            LinkedList<Integer> proximityList = intermediateCluster.getZoneById(zoneId)
                                                                   .getProximityList();
            proximityList.remove(new Integer(dropZoneId));
            zones.add(new Zone(zoneId, proximityList));
        }

        Cluster finalCluster = new Cluster(intermediateCluster.getName(),
                                           Utils.asSortedList(nodes),
                                           Utils.asSortedList(zones));

        // Make sure everything is fine
        if(initialCluster.getNumberOfPartitions() != finalCluster.getNumberOfPartitions()) {
            System.out.println("ERROR : The number of partitions in the initial and the final cluster is not equal \n");
        }

        Set<Integer> finalPartitions = new HashSet<Integer>();
        for(Integer zoneId: finalCluster.getZoneIds()) {
            finalPartitions.addAll(finalCluster.getPartitionIdsInZone(zoneId));
        }

        // Compare to original partition ids list
        if(!originalPartitions.equals(finalPartitions)) {
            System.out.println("ERROR : The list of partition ids in the initial and the final cluster doesn't match \n ");
        }

        // Finally write the final cluster to a xml file
        RebalanceUtils.dumpClusterToFile(outputDir,
                                         RebalanceUtils.finalClusterFileName,
                                         finalCluster);
    }
}
