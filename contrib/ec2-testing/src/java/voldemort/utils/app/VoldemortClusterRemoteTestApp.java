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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import joptsimple.OptionSet;
import voldemort.utils.CmdUtils;
import voldemort.utils.CommandLineClusterConfig;
import voldemort.utils.RemoteTestResult;
import voldemort.utils.RemoteTestSummarizer;
import voldemort.utils.SshRemoteTest;

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
        parser.accepts("sshprivatekey", "File containing SSH private key").withRequiredArg();
        parser.accepts("hostuserid", "User ID on remote host").withRequiredArg();
        parser.accepts("voldemortroot", "Voldemort's root directory on remote host")
              .withRequiredArg();
        parser.accepts("voldemorthome", "Voldemort's home directory on remote host")
              .withRequiredArg();
        parser.accepts("start-key-index",
                       "Value of --start-key-index for voldemort-remote-test.sh on remote host")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("numrequests",
                       "Value of numrequests for voldemort-remote-test.sh on remote host")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("iterations",
                       "Value of --iterations for voldemort-remote-test.sh on remote host")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("value-size",
                       "Value of --value-size for voldemort-remote-test.sh on remote host")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("threads", "Value of --threads for voldemort-remote-test.sh on remote host")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("operations",
                       "Value of -r, -w, and/or -d for voldemort-remote-test.sh on remote host")
              .withRequiredArg();
        parser.accepts("storename",
                       "Value of store name for voldemort-remote-test.sh on remote host, defaults to \"test\"")
              .withRequiredArg();
        parser.accepts("bootstrapurl",
                       "Value of bootstrap-url for voldemort-remote-test.sh on remote host")
              .withRequiredArg();

        OptionSet options = parse(args);
        File hostNamesFile = getRequiredInputFile(options, "hostnames");
        File sshPrivateKey = getRequiredInputFile(options, "sshprivatekey");
        String hostUserId = CmdUtils.valueOf(options, "hostuserid", "root");
        String voldemortHomeDirectory = getRequiredString(options, "voldemorthome");
        String voldemortRootDirectory = getRequiredString(options, "voldemortroot");
        int startKeyIndex = CmdUtils.valueOf(options, "start-key-index", 0);
        int numRequests = getRequiredInt(options, "numrequests");
        int iterations = CmdUtils.valueOf(options, "iterations", 1);
        int valueSize = CmdUtils.valueOf(options, "value-size", 1024);
        int threads = CmdUtils.valueOf(options, "threads", 8);
        String operations = getRequiredString(options, "operations");
        String bootstrapUrl = getRequiredString(options, "bootstrapurl");
        String storeName = CmdUtils.valueOf(options, "storename", "test");

        List<String> publicHostNames = getHostNamesFromFile(hostNamesFile, true);

        CommandLineClusterConfig config = new CommandLineClusterConfig();
        config.setHostNames(publicHostNames);
        config.setHostUserId(hostUserId);
        config.setSshPrivateKey(sshPrivateKey);
        config.setVoldemortHomeDirectory(voldemortHomeDirectory);
        config.setVoldemortRootDirectory(voldemortRootDirectory);

        Map<String, String> remoteTestArguments = new HashMap<String, String>();

        for(String publicHostName: publicHostNames) {
            remoteTestArguments.put(publicHostName, "-" + operations + " --start-key-index "
                                                    + (startKeyIndex * numRequests)
                                                    + " --value-size " + valueSize + " --threads "
                                                    + threads + " --iterations " + iterations + " "
                                                    + bootstrapUrl + " " + storeName + " "
                                                    + numRequests);
            startKeyIndex++;
        }

        config.setRemoteTestArguments(remoteTestArguments);

        List<RemoteTestResult> remoteTestResults = new SshRemoteTest(config).execute();
        new RemoteTestSummarizer().outputTestResults(remoteTestResults);
    }

}