package voldemort.utils.ec2testing;

import java.io.IOException;
import java.io.PrintStream;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.utils.CmdUtils;

import com.xerox.amazonws.ec2.InstanceType;

public class Ec2InstanceCreator {

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("accessid", "AccessID.  (required)").withRequiredArg();
        parser.accepts("secretkey", "SecretKey.  (required)").withRequiredArg();
        parser.accepts("ami", "AMI.  (required)").withRequiredArg();
        parser.accepts("keypairid", "KeyPairID  (required)").withRequiredArg();
        parser.accepts("instances", "Number of instances. default 1")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("timeout",
                       "Timeout time(in milliseconds), default = 1 minute + 30 seconds for each instance")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("size",
                       "VMI size. Size options are DEFAULT, LARGE, XLARGE, MEDIUM_HCPU, and XLARGE_HCPU, default value is DEFAULT")
              .withRequiredArg();

        OptionSet options = parser.parse(args);

        if(!options.has("accessid"))
            printUsage(System.err, parser);

        String accessId = CmdUtils.valueOf(options, "accessid", "");

        if(!options.has("secretkey"))
            printUsage(System.err, parser);

        String secretKey = CmdUtils.valueOf(options, "secretkey", "");

        if(!options.has("ami"))
            printUsage(System.err, parser);

        String ami = CmdUtils.valueOf(options, "ami", "");

        if(!options.has("keypairid"))
            printUsage(System.err, parser);

        String keypairId = CmdUtils.valueOf(options, "keypairid", "");
        int instanceCount = CmdUtils.valueOf(options, "instances", 1);
        int timeout = CmdUtils.valueOf(options, "timeout", (60000 + instanceCount * 30000));
        String instanceSize = CmdUtils.valueOf(options, "size", "DEFAULT");
        try {
            InstanceType.valueOf(instanceSize);
        } catch(Exception e) {
            printUsage(System.err, parser);
        }
        System.out.println(timeout);
        for(int i = 0; i < args.length; i++) {
            System.out.println(args[i]);
        }

        Ec2Connection ec2Connection = new TypicaEc2Connection(accessId, secretKey);
        ec2Connection.createInstances(ami, keypairId, instanceSize, instanceCount, timeout);
    }

    private static void printUsage(PrintStream out, OptionParser parser) throws IOException {
        out.println("Usage: $VOLDEMORT_HOME/contrib/ec2-testing/bin/voldemort-ec2instancecreator.sh");
        parser.printHelpOn(out);
        System.exit(1);
    }

}