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
import voldemort.utils.SshClusterCleaner;

public class VoldemortClusterCleanerApp extends VoldemortApp {

    public static void main(String[] args) throws Exception {
        new VoldemortClusterCleanerApp().run(args);
    }

    @Override
    protected String getScriptName() {
        return "voldemort-clustercleaner.sh";
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
        parser.accepts("voldemorthome", "Voldemort's home directory on remote host")
              .withRequiredArg();

        OptionSet options = parse(args);
        File hostNamesFile = getRequiredInputFile(options, "hostnames");
        File sshPrivateKey = getRequiredInputFile(options, "sshprivatekey");
        String hostUserId = CmdUtils.valueOf(options, "hostuserid", "root");
        String voldemortHomeDirectory = getRequiredString(options, "voldemorthome");

        List<String> hostNames = getHostNamesFromFile(hostNamesFile, true);

        final CommandLineClusterConfig config = new CommandLineClusterConfig();
        config.setHostNames(hostNames);
        config.setHostUserId(hostUserId);
        config.setSshPrivateKey(sshPrivateKey);
        config.setVoldemortHomeDirectory(voldemortHomeDirectory);

        ClusterOperation<Object> operation = new SshClusterCleaner(config);
        operation.execute();
    }

}
