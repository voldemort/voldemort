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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import joptsimple.OptionSet;
import voldemort.utils.CmdUtils;
import voldemort.utils.HostNamePair;
import voldemort.utils.RemoteOperation;
import voldemort.utils.impl.SshRemoteTest;

public class VoldemortClusterRemoteTestApp extends VoldemortApp {

    public static void main(String[] args) throws Exception {
        new VoldemortClusterRemoteTestApp().run(args);
    }

    @Override
    protected String getScriptName() {
        return "voldemort-clusterremotetest.sh";
    }

    @Override
    public void run(String[] args) throws Exception {
        parser.accepts("help", "Prints this help");
        parser.accepts("logging",
                       "Options are \"debug\", \"info\" (default), \"warn\", \"error\", or \"off\"")
              .withRequiredArg();
        parser.accepts("hostnames", "File containing host names").withRequiredArg();
        parser.accepts("sshprivatekey", "File containing SSH private key (optional)")
              .withRequiredArg();
        parser.accepts("hostuserid", "User ID on remote host").withRequiredArg();
        parser.accepts("commands", "File containing per-host commands").withRequiredArg();

        OptionSet options = parse(args);
        List<HostNamePair> hostNamePairs = getHostNamesPairsFromFile(getRequiredInputFile(options,
                                                                                          "hostnames"));
        List<String> hostNames = new ArrayList<String>();

        for(HostNamePair hostNamePair: hostNamePairs)
            hostNames.add(hostNamePair.getExternalHostName());

        File sshPrivateKey = getInputFile(options, "sshprivatekey");
        String hostUserId = CmdUtils.valueOf(options, "hostuserid", "root");
        Map<String, String> commands = getRequiredPropertiesFile(getRequiredInputFile(options,
                                                                                      "commands"));

        RemoteOperation operation = new SshRemoteTest(hostNames,
                                                      sshPrivateKey,
                                                      hostUserId,
                                                      commands);
        operation.execute();
    }

}