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
import voldemort.utils.CmdUtils;
import voldemort.utils.SshClusterStarter;
import voldemort.utils.ClusterStarter;

public class VoldemortClusterStarterApp {

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("hostsfile", "File containing list of remote host names.").withRequiredArg();
        parser.accepts("sshprivatekey", "File containing private SSH key").withRequiredArg();
        parser.accepts("voldemortroot", "Voldemort's root directory on remote host")
              .withRequiredArg();
        parser.accepts("voldemorthome", "Voldemort's home directory").withRequiredArg();
        parser.accepts("hostname", "User name on remote host. Default root").withRequiredArg();

        OptionSet options = parser.parse(args);

        if(!options.has("hostsfile"))
            printUsage(System.err, parser);

        String hostsFile = CmdUtils.valueOf(options, "hostsfile", "");
        File file = new File(hostsFile);
        if(!file.canRead()) {
            System.out.println("Hosts File cannot be read.");
            System.exit(2);
        }

        if(!options.has("voldemorthome"))
            printUsage(System.err, parser);

        String voldemortHomeDirectory = CmdUtils.valueOf(options, "voldemorthome", "");

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

        if(!options.has("voldemortroot"))
            printUsage(System.err, parser);

        String voldemortRootDirectory = CmdUtils.valueOf(options, "voldemortroot", "");

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

        ClusterStarter voldemortClusterStarter = new SshClusterStarter();

        voldemortClusterStarter.start(hostNames,
                                      hostUserId,
                                      sshPrivateKey,
                                      voldemortRootDirectory,
                                      voldemortHomeDirectory,
                                      600000);
    }

    private static void printUsage(PrintStream out, OptionParser parser) throws IOException {
        out.println("Usage: $VOLDEMORT_HOME/contrib/ec2-testing/bin/voldemort-clusterstarter.sh");
        parser.printHelpOn(out);
        System.exit(1);
    }
}
