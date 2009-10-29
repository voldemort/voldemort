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
import java.util.List;

import joptsimple.OptionSet;

import org.apache.commons.io.FileUtils;

import voldemort.utils.CmdUtils;
import voldemort.utils.Ec2Connection;
import voldemort.utils.HostNamePair;
import voldemort.utils.impl.TypicaEc2Connection;

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
        parser.accepts("instancetype",
                       "Instance type; options are " + Ec2Connection.Ec2InstanceType.DEFAULT
                               + " (default), " + Ec2Connection.Ec2InstanceType.LARGE + ", "
                               + Ec2Connection.Ec2InstanceType.XLARGE + ", "
                               + Ec2Connection.Ec2InstanceType.MEDIUM_HCPU + ", and "
                               + Ec2Connection.Ec2InstanceType.XLARGE_HCPU).withRequiredArg();
        parser.accepts("output",
                       "Output file for newly created external and internal host name entries")
              .withRequiredArg();

        OptionSet options = parse(args);
        String accessId = getRequiredString(options, "accessid");
        String secretKey = getRequiredString(options, "secretkey");
        String ami = getRequiredString(options, "ami");
        String keypairId = getRequiredString(options, "keypairid");
        int instanceCount = CmdUtils.valueOf(options, "instances", 1);
        Ec2Connection.Ec2InstanceType instanceType = null;

        try {
            instanceType = Ec2Connection.Ec2InstanceType.valueOf(CmdUtils.valueOf(options,
                                                                                  "instancetype",
                                                                                  "DEFAULT"));
        } catch(Exception e) {
            printUsage();
        }

        File output = getRequiredOutputFile(options, "output");

        Ec2Connection ec2Connection = new TypicaEc2Connection(accessId, secretKey);
        List<HostNamePair> hostNamePairs = ec2Connection.create(ami,
                                                                keypairId,
                                                                instanceType,
                                                                instanceCount);

        StringBuilder s = new StringBuilder();

        for(HostNamePair hostNamePair: hostNamePairs) {
            s.append(hostNamePair.getExternalHostName());
            s.append(',');
            s.append(hostNamePair.getInternalHostName());
            s.append(System.getProperty("line.separator"));
        }

        FileUtils.writeStringToFile(output, s.toString());
    }

}