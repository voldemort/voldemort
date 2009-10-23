/*
 * Copyright 2009 LinkedIn, Inc.
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

package voldemort.utils.app;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.utils.CmdUtils;
import voldemort.utils.Ec2Connection;
import voldemort.utils.TypicaEc2Connection;

import com.xerox.amazonws.ec2.InstanceType;

public class VoldemortEc2InstanceCreatorApp {

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
                       "Timeout time(in milliseconds), default = 2 minute + 30 seconds for each instance")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("size",
                       "VMI size. Size options are DEFAULT, LARGE, XLARGE, MEDIUM_HCPU, and XLARGE_HCPU, default value is DEFAULT")
              .withRequiredArg();
        parser.accepts("fileout", "The file that you want to output the public and private DNS to.")
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

        if(!options.has("fileout"))
            printUsage(System.err, parser);

        String fileOut = CmdUtils.valueOf(options, "fileout", "");
        File file = new File(fileOut);
        File parentDirectory = file.getAbsoluteFile().getParentFile();

        // Try to make any parent directories for the user. Don't bother
        // checking here as we'll determine writability right below.
        parentDirectory.mkdirs();

        if(!parentDirectory.canWrite()) {
            System.out.println("File cannot be written in directory "
                               + parentDirectory.getAbsolutePath());
            System.exit(2);
        }

        if(!options.has("keypairid"))
            printUsage(System.err, parser);

        String keypairId = CmdUtils.valueOf(options, "keypairid", "");
        int instanceCount = CmdUtils.valueOf(options, "instances", 1);
        int timeout = CmdUtils.valueOf(options, "timeout", (120000 + instanceCount * 30000));
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
        Map<String, String> dnsNames = ec2Connection.createInstances(ami,
                                                                     keypairId,
                                                                     instanceSize,
                                                                     instanceCount,
                                                                     timeout);

        PrintStream out = null;

        try {
            out = new PrintStream(new FileOutputStream(file));
            Iterator<String> it = dnsNames.keySet().iterator();
            while(it.hasNext()) {
                String publicDns = it.next();
                String privateDns = dnsNames.get(publicDns);
                String dnsName = publicDns + "," + privateDns;
                System.out.println(dnsName);
                out.println(dnsName);
            }
        } finally {
            if(out != null)
                out.close();
        }
    }

    private static void printUsage(PrintStream out, OptionParser parser) throws IOException {
        out.println("Usage: $VOLDEMORT_HOME/contrib/ec2-testing/bin/voldemort-ec2instancecreator.sh");
        parser.printHelpOn(out);
        System.exit(1);
    }

}