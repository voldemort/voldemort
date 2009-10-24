package voldemort.utils.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.utils.ClusterOperation;
import voldemort.utils.CmdUtils;
import voldemort.utils.CommandLineClusterConfig;
import voldemort.utils.RsyncDeployer;

public class VoldemortDeployerApp {

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("hostsfile", "File containing list of remote host names.").withRequiredArg();
        parser.accepts("hostuserid", "Name of user on remote host. Default root").withRequiredArg();
        parser.accepts("sshprivatekey", "File containing the private SSH key").withRequiredArg();
        parser.accepts("voldemortparent", "Voldemort's parent directory on remote host")
              .withRequiredArg();
        parser.accepts("sourcedir", "The source directory on the local machine").withRequiredArg();

        OptionSet options = parser.parse(args);

        if(!options.has("hostsfile"))
            printUsage(System.err, parser);

        String hostsFile = CmdUtils.valueOf(options, "hostsfile", "");
        File file = new File(hostsFile);
        if(!file.canRead()) {
            System.out.println("Hosts File cannot be read.");
            System.exit(2);
        }

        if(!options.has("sshprivatekey"))
            printUsage(System.err, parser);

        String sshKey = CmdUtils.valueOf(options, "sshprivatekey", "");

        File sshPrivateKey = new File(sshKey);

        if(!sshPrivateKey.canRead()) {
            System.out.println("SSH Private Key File cannot be read.");
            System.exit(2);
        }

        String hostUserId = "";
        if(options.has("hostuserid"))
            hostUserId = CmdUtils.valueOf(options, "hostuserid", "");
        else
            hostUserId = "root";

        if(!options.has("voldemortparent"))
            printUsage(System.err, parser);

        String voldemortParentDirectory = CmdUtils.valueOf(options, "voldemortparent", "");

        if(!options.has("sourcedir"))
            printUsage(System.err, parser);

        String sourceDir = CmdUtils.valueOf(options, "sourcedir", "");
        File sourceDirectory = new File(sourceDir);
        if(!sourceDirectory.canRead()) {
            System.out.println("Source Directory cannot be read.");
            System.exit(2);
        }

        List<String> hostNames = new ArrayList<String>();
        try {
            BufferedReader in = new BufferedReader(new FileReader(file));
            String str;
            while((str = in.readLine()) != null) {
                if(str.indexOf(',') != -1)
                    hostNames.add(str.substring(0, str.indexOf(",")));
                else
                    hostNames.add(str);
            }
            in.close();
        } catch(IOException e) {}

        CommandLineClusterConfig config = new CommandLineClusterConfig();
        config.setHostNames(hostNames);
        config.setHostUserId(hostUserId);
        config.setSshPrivateKey(sshPrivateKey);
        config.setVoldemortParentDirectory(voldemortParentDirectory);
        config.setSourceDirectory(sourceDirectory);

        ClusterOperation operation = new RsyncDeployer(config);
        operation.execute();
    }

    private static void printUsage(PrintStream out, OptionParser parser) throws IOException {
        out.println("Usage: $VOLDEMORT_HOME/contrib/ec2-testing/bin/voldemort-deployer.sh");
        parser.printHelpOn(out);
        System.exit(1);
    }
}
