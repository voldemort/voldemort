package voldemort.utils.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.utils.ClusterGenerator;
import voldemort.utils.CmdUtils;

public class VoldemortClusterGeneratorApp {

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("dnsfile", "File containing public and private dns names.")
              .withRequiredArg();
        parser.accepts("partitions", "Number of partitions for each cluster node.")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("fileout", "cluster.xml configuration file. Default System.out")
              .withRequiredArg();

        OptionSet options = parser.parse(args);

        if(!options.has("dnsfile"))
            printUsage(System.err, parser);

        String dnsFile = CmdUtils.valueOf(options, "dnsfile", "");
        File file = new File(dnsFile);
        if(!file.canRead()) {
            System.out.println("Dns File cannot be read.");
            System.exit(2);
        }

        if(!options.has("partitions"))
            printUsage(System.err, parser);

        int partitions = CmdUtils.valueOf(options, "partitions", 0);

        File outFile = null;

        if(options.has("fileout")) {

            String fileOut = CmdUtils.valueOf(options, "fileout", "");
            outFile = new File(fileOut);
            File parentDirectory = outFile.getAbsoluteFile().getParentFile();

            // Try to make any parent directories for the user. Don't bother
            // checking here as we'll determine writability right below.
            parentDirectory.mkdirs();

            if(!parentDirectory.canWrite()) {
                System.out.println("File cannot be written in directory "
                                   + parentDirectory.getAbsolutePath());
                System.exit(2);
            }
        }
        List<String> dnsNames = new ArrayList<String>();
        try {
            BufferedReader in = new BufferedReader(new FileReader(file));
            String str;
            while((str = in.readLine()) != null) {
                dnsNames.add(str.substring(str.indexOf(",") + 1, str.length()));
            }
            in.close();
        } catch(IOException e) {}

        ClusterGenerator cdg = new ClusterGenerator();
        String descriptor = cdg.createClusterDescriptor(dnsNames, partitions);

        PrintStream out = null;
        if(outFile != null) {
            try {
                out = new PrintStream(new FileOutputStream(outFile));
                out.print(descriptor);
            } finally {
                if(out != null)
                    out.close();
            }
        } else {
            System.out.print(descriptor);
        }
    }

    private static void printUsage(PrintStream out, OptionParser parser) throws IOException {
        out.println("Usage: $VOLDEMORT_HOME/contrib/ec2-testing/bin/voldemort-clustergenerator.sh");
        parser.printHelpOn(out);
        System.exit(1);
    }
}
