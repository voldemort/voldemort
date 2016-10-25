package voldemort.tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Random;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Node;
import voldemort.tools.admin.AdminToolUtils;
import voldemort.utils.Utils;


public class GenerateScriptCLI {

    private static OptionParser setupParser() {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "Print usage information").withOptionalArg();
        parser.acceptsAll(Arrays.asList("s", "script"), "Script")
              .withRequiredArg()
              .describedAs("script")
              .ofType(String.class);
        parser.acceptsAll(Arrays.asList("u", "url"), "bootstrapUrl")
              .withRequiredArg()
              .describedAs("url")
              .ofType(String.class);
        parser.acceptsAll(Arrays.asList("scp", "scpFile"), "file to be scp ed")
              .withRequiredArg()
              .describedAs("scp")
              .ofType(String.class);
        parser.acceptsAll(Arrays.asList("o", "output"), "outputScript")
              .withRequiredArg()
              .describedAs("output")
              .ofType(String.class);
        return parser;
    }

    private static void printUsage() {
        StringBuilder help = new StringBuilder();
        help.append("GenerateScriptCLI\n");
        help.append("  Given a script, Generates a new script which will use SSH to run the given \n");
        help.append(" script on all hosts in a given cluster. The variable @@ID@@ , @@HOST@@ , @@URL@@ \n");
        help.append(" will be replaced with the node id, host name and bootstrap Url respectively\n");
        help.append("Options:\n");
        help.append("  Required:\n");
        help.append("    --url <bootstrapUrl>\n");
        help.append("    --script <Script to run on each host>\n");
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

        if(!options.hasArgument("url") || !options.hasArgument("script")
           || !options.hasArgument("output")) {
            printUsageAndDie("Missing a required argument.");
            return;
        }
        
        String url = (String) options.valueOf("url");
        String inputScriptPath = getFilePath(options, "script");
        String outputScriptPath = getFilePath(options, "output");
        String scpFilePath = getFilePath(options, "scp");

        AdminClient client = AdminToolUtils.getAdminClient(url);

        PrintWriter writer = new PrintWriter(outputScriptPath, "UTF-8");
        for(Node node: client.getAdminClientCluster().getNodes()) {
            FileInputStream fis = new FileInputStream(inputScriptPath);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));

            if(scpFilePath != null && scpFilePath.length() > 0) {
                writer.println("scp " + scpFilePath + "  " + node.getHost() + ":~");
            }
            int randomNumber = 1000 + new Random().nextInt(100000);
            String hereDocumentTag = "NODE_" + node.getId() + "_" + randomNumber;
            // Use SSH here script
            writer.println("ssh -T " + node.getHost() + "  << " + hereDocumentTag);
            String line = null;
            while((line = br.readLine()) != null) {
                line = line.replace("@@ID@@", Integer.toString(node.getId()));
                line = line.replace("@@HOST@@", node.getHost());
                line = line.replace("@@URL@@", node.getSocketUrl().toString());
                writer.println(line);
            }

            writer.println(hereDocumentTag);
            writer.println("\n\n\n");
            br.close();
        }
        writer.close();
        System.out.println("Output script generated at " + outputScriptPath);
    }
}
