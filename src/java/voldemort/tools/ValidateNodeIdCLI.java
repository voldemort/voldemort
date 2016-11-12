package voldemort.tools;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.VoldemortApplicationException;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;

import com.google.common.io.Files;


public class ValidateNodeIdCLI {

    private static OptionParser setupParser() {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "Print usage information").withOptionalArg();
        parser.acceptsAll(Arrays.asList("id", "nodeId"), "expected node Id")
              .withRequiredArg()
              .describedAs("expected node Id")
              .ofType(String.class);
        parser.acceptsAll(Arrays.asList("path", "clusterPath"), "clusterPath")
              .withRequiredArg()
              .describedAs("clusterPath")
              .ofType(String.class);
        return parser;
    }

    private static void printUsage() {
        StringBuilder help = new StringBuilder();
        help.append("ValidateNodeIdCLI\n");
        help.append("  Validate if the auto detection agrees with the expected node Id \n");
        help.append("Options:\n");
        help.append("  Required:\n");
        help.append("    --id <expected nodeId>\n");
        help.append("    --path <comma separated list of file paths>\n");
        System.out.print(help.toString());
    }

    private static void printUsageAndDie(String errMessage) {
        printUsage();
        Utils.croak("\n" + errMessage);
    }

    public static String getFilePath(OptionSet options, String name) {
        String path = (String) options.valueOf(name);
        return path.replace("~", System.getProperty("user.home"));
    }

    private static Cluster getCluster(File clusterXML) throws IOException {
        return new ClusterMapper().readCluster(clusterXML);
    }

    private static String getTempDirPath() {
        File tempdir = Files.createTempDir();
        tempdir.delete();
        tempdir.mkdir();
        tempdir.deleteOnExit();
        return tempdir.getAbsolutePath();
    }

    public static void main(String[] args) throws IOException {
        OptionParser parser = null;
        OptionSet options = null;
        try {
            parser = setupParser();
            options = parser.parse(args);
        } catch(OptionException oe) {
            parser.printHelpOn(System.out);
            printUsageAndDie("Exception when parsing arguments : " + oe.getMessage());
            return;
        }

        /* validate options */
        if(options.has("help")) {
            printUsage();
            return;
        }

        if(!options.hasArgument("id") || !options.hasArgument("path")) {
            printUsageAndDie("Missing a required argument.");
            return;
        }
        String id = (String) options.valueOf("id");
        int expectedNodeId = Integer.parseInt(id);

        String clusterPaths = getFilePath(options, "path");
        String[] allPaths = clusterPaths.split(",");
        boolean isMatch = false;
        for(String path: allPaths) {
            path = path.replace("~", System.getProperty("user.home"));
            File filePath = new File(path);
            if(filePath.exists()) {
                isMatch = true;
                Cluster cluster = getCluster(filePath);
                
                Properties properties = new Properties();
                properties.setProperty(VoldemortConfig.ENABLE_NODE_ID_DETECTION,
                                       Boolean.toString(true));
                properties.setProperty(VoldemortConfig.VOLDEMORT_HOME, getTempDirPath());
                VoldemortConfig config = new VoldemortConfig(properties);

                int actualNodeId = VoldemortServer.getNodeId(config, cluster);
                if(actualNodeId != expectedNodeId) {
                    throw new VoldemortApplicationException("Mismatch detected. Computed Node Id "
                                                            + actualNodeId + " Expected "
                                                            + expectedNodeId);
                } else {
                    System.out.println("Expected and Computed node Id matched " + expectedNodeId);
                }

                config.setNodeId(actualNodeId);
                VoldemortServer.validateNodeId(config, cluster);
                System.out.println("Validation of node Id passed");
            }
        }

        if(!isMatch) {
            throw new VoldemortApplicationException("None of the paths matched the cluster.xml");
        }
    }
}
