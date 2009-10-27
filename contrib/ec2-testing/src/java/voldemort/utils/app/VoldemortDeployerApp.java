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
import voldemort.utils.ClusterOperation;
import voldemort.utils.CmdUtils;
import voldemort.utils.CommandLineClusterConfig;
import voldemort.utils.RsyncDeployer;

public class VoldemortDeployerApp extends VoldemortApp {

    public static void main(String[] args) throws Exception {
        new VoldemortDeployerApp().run(args);
    }

    @Override
    protected String getScriptName() {
        return "voldemort-deployer.sh";
    }

    @Override
    public void run(String[] args) throws Exception {
        parser.accepts("help", "Prints this help");
        parser.accepts("logging",
                       "Options are \"debug\", \"info\" (default), \"warn\", \"error\", or \"off\"")
              .withRequiredArg();
        parser.accepts("hostnames", "File containing host names").withRequiredArg();
        parser.accepts("sshprivatekey", "File containing SSH private key").withRequiredArg();
        parser.accepts("hostuserid", "User ID on remote host").withRequiredArg();
        parser.accepts("voldemortparent", "Voldemort's parent directory on remote host")
              .withRequiredArg();
        parser.accepts("source", "The source directory on the local machine").withRequiredArg();

        OptionSet options = parse(args);
        File hostNamesFile = getRequiredInputFile(options, "hostnames");
        File sshPrivateKey = getRequiredInputFile(options, "sshprivatekey");
        String hostUserId = CmdUtils.valueOf(options, "hostuserid", "root");
        String voldemortParentDirectory = getRequiredString(options, "voldemortparent");
        File sourceDirectory = getRequiredInputFile(options, "source");

        List<String> hostNames = getHostNamesFromFile(hostNamesFile, true);

        CommandLineClusterConfig config = new CommandLineClusterConfig();
        config.setHostNames(hostNames);
        config.setHostUserId(hostUserId);
        config.setSshPrivateKey(sshPrivateKey);
        config.setVoldemortParentDirectory(voldemortParentDirectory);
        config.setSourceDirectory(sourceDirectory);

        ClusterOperation<Object> operation = new RsyncDeployer(config);
        operation.execute();
    }

}
