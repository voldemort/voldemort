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
import java.util.Map;

import joptsimple.OptionSet;

import org.apache.commons.io.FileUtils;

import voldemort.utils.CmdUtils;
import voldemort.utils.Ec2Connection;
import voldemort.utils.TypicaEc2Connection;

import com.xerox.amazonws.ec2.InstanceType;

public class VoldemortEc2InstanceCreatorApp extends VoldemortApp {

    public static void main(String[] args) throws Exception {
        new VoldemortEc2InstanceCreatorApp().run(args);
    }

    @Override
    protected String getScriptName() {
        return "voldemort-ec2instancecreator.sh";
    }

    @Override
    public void run(String[] args) throws Exception {
        parser.accepts("help", "Prints this help");
        parser.accepts("logging",
                       "Options are \"debug\", \"info\" (default), \"warn\", \"error\", or \"off\"")
              .withRequiredArg();
        parser.accepts("accessid", "Access ID").withRequiredArg();
        parser.accepts("secretkey", "SecretKey").withRequiredArg();
        parser.accepts("ami", "AMI").withRequiredArg();
        parser.accepts("keypairid", "KeyPairID").withRequiredArg();
        parser.accepts("instances", "Number of instances (default 1)")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("instancesize",
                       "Instance size; options are DEFAULT (default), LARGE, XLARGE, MEDIUM_HCPU, and XLARGE_HCPU")
              .withRequiredArg();
        parser.accepts("output",
                       "Output of newly created public and private DNS entries; defaults to stdout")
              .withRequiredArg();

        OptionSet options = parse(args);
        String accessId = getRequiredString(options, "accessid");
        String secretKey = getRequiredString(options, "secretkey");
        String ami = getRequiredString(options, "ami");
        String keypairId = getRequiredString(options, "keypairid");
        int instanceCount = CmdUtils.valueOf(options, "instances", 1);
        String instanceSize = CmdUtils.valueOf(options, "instancesize", "DEFAULT");
        File outputFile = getOutputFile(options, "output");

        try {
            InstanceType.valueOf(instanceSize);
        } catch(Exception e) {
            printUsage();
        }

        Ec2Connection ec2Connection = new TypicaEc2Connection(accessId, secretKey);
        Map<String, String> dnsNames = ec2Connection.createInstances(ami,
                                                                     keypairId,
                                                                     instanceSize,
                                                                     instanceCount);

        StringBuilder dnsNameEntriesBuilder = new StringBuilder();

        for(Map.Entry<String, String> entry: dnsNames.entrySet()) {
            dnsNameEntriesBuilder.append(entry.getKey());
            dnsNameEntriesBuilder.append(',');
            dnsNameEntriesBuilder.append(entry.getValue());
            dnsNameEntriesBuilder.append(System.getProperty("line.separator"));
        }

        String dnsNameEntries = dnsNameEntriesBuilder.toString();

        if(outputFile != null)
            FileUtils.writeStringToFile(outputFile, dnsNameEntries);
        else
            System.out.print(dnsNameEntries);

    }

}