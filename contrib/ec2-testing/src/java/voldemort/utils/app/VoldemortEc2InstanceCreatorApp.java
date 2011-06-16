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

import java.util.Arrays;
import java.util.List;

import joptsimple.OptionSet;
import voldemort.utils.CmdUtils;
import voldemort.utils.Ec2Connection;
import voldemort.utils.HostNamePair;
import voldemort.utils.Utils;
import voldemort.utils.impl.TypicaEc2Connection;

import com.xerox.amazonws.ec2.RegionInfo;

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
        parser.accepts("accessid", "Access ID (used instead of accessidfile)").withRequiredArg();
        parser.accepts("accessidfile", "Access ID file (used instead of accessid)")
              .withRequiredArg();
        parser.accepts("secretkey", "Secret key (used instead of secretkeyfile)").withRequiredArg();
        parser.accepts("secretkeyfile", "Secret key file (used instead of secretkey)")
              .withRequiredArg();
        parser.accepts("securitygroups", "Security groups to allow on instances (optional)")
              .withRequiredArg();
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
        parser.accepts("region",
                       "Region type; options are " + RegionInfo.REGIONURL_AP_SOUTHEAST + ", "
                               + RegionInfo.REGIONURL_EU_WEST + ", " + RegionInfo.REGIONURL_US_WEST
                               + ", " + RegionInfo.REGIONURL_US_EAST + " (default) ")
              .withRequiredArg();

        OptionSet options = parse(args);
        String accessId = getAccessId(options);
        String secretKey = getSecretKey(options);
        String ami = getRequiredString(options, "ami");
        String keypairId = getRequiredString(options, "keypairid");
        String regionUrl = getRegionUrl(options);
        int instanceCount = CmdUtils.valueOf(options, "instances", 1);
        String securityGroups = CmdUtils.valueOf(options, "securitygroups", null);
        List<String> securityGroupsList = (securityGroups != null) ? Arrays.asList(securityGroups.split(","))
                                                                  : null;
        Ec2Connection.Ec2InstanceType instanceType = null;

        try {
            instanceType = Ec2Connection.Ec2InstanceType.valueOf(CmdUtils.valueOf(options,
                                                                                  "instancetype",
                                                                                  "DEFAULT"));
        } catch(Exception e) {
            printUsage();
        }

        Ec2Connection ec2Connection = new TypicaEc2Connection(accessId, secretKey, null, regionUrl);
        List<HostNamePair> hostNamePairs = ec2Connection.createInstances(ami,
                                                                         keypairId,
                                                                         instanceType,
                                                                         instanceCount,
                                                                         securityGroupsList);

        StringBuilder s = new StringBuilder();

        for(HostNamePair hostNamePair: hostNamePairs) {
            s.append(hostNamePair.getExternalHostName());
            s.append('=');
            s.append(hostNamePair.getInternalHostName());
            s.append(Utils.NEWLINE);
        }

        System.out.print(s);
    }

}
