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

import joptsimple.OptionSet;
import voldemort.utils.CmdUtils;
import voldemort.utils.HostNamePair;
import voldemort.utils.RemoteTestResult;
import voldemort.utils.RemoteTestResult.RemoteTestIteration;
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
        parser.accepts("sshprivatekey", "File containing SSH private key").withRequiredArg();
        parser.accepts("hostuserid", "User ID on remote host").withRequiredArg();
        parser.accepts("voldemortroot", "Voldemort's root directory on remote host")
              .withRequiredArg();
        parser.accepts("voldemorthome", "Voldemort's home directory on remote host")
              .withRequiredArg();
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
        parser.accepts("ramp",
                       "Number of seconds to sleep to stagger clients before running test on remote host, defaults to 30")
              .withRequiredArg()
              .ofType(Integer.class);

        OptionSet options = parse(args);
        List<HostNamePair> hostNamePairs = getHostNamesPairsFromFile(getRequiredInputFile(options,
                                                                                          "hostnames"));
        List<String> hostNames = new ArrayList<String>();

        for(HostNamePair hostNamePair: hostNamePairs)
            hostNames.add(hostNamePair.getExternalHostName());

        File sshPrivateKey = getRequiredInputFile(options, "sshprivatekey");
        String hostUserId = CmdUtils.valueOf(options, "hostuserid", "root");
        String voldemortRootDirectory = getRequiredString(options, "voldemortroot");
        String voldemortHomeDirectory = getRequiredString(options, "voldemorthome");
        int rampTime = CmdUtils.valueOf(options, "ramp", 30);
        String operations = getRequiredString(options, "operations");
        int valueSize = CmdUtils.valueOf(options, "value-size", 1024);
        int threads = CmdUtils.valueOf(options, "threads", 8);
        int iterations = CmdUtils.valueOf(options, "iterations", 1);
        String bootstrapUrl = getRequiredString(options, "bootstrapurl");
        String storeName = CmdUtils.valueOf(options, "storename", "test");
        long numRequests = getRequiredInt(options, "numrequests");

        List<RemoteTestResult> remoteTestResults = new SshRemoteTest(hostNames,
                                                                     sshPrivateKey,
                                                                     hostUserId,
                                                                     voldemortRootDirectory,
                                                                     voldemortHomeDirectory,
                                                                     rampTime,
                                                                     operations,
                                                                     valueSize,
                                                                     threads,
                                                                     iterations,
                                                                     bootstrapUrl,
                                                                     storeName,
                                                                     numRequests).execute();
        outputTestResults(remoteTestResults);
    }

    private void outputTestResults(List<RemoteTestResult> remoteTestResults) {
        List<Double> totalReadResults = new ArrayList<Double>();
        List<Double> totalWriteResults = new ArrayList<Double>();
        List<Double> totalDeleteResults = new ArrayList<Double>();

        for(RemoteTestResult remoteTestResult: remoteTestResults) {
            List<Double> hostReadResults = new ArrayList<Double>();
            List<Double> hostWriteResults = new ArrayList<Double>();
            List<Double> hostDeleteResults = new ArrayList<Double>();

            for(RemoteTestIteration remoteTestIteration: remoteTestResult.getRemoteTestIterations()
                                                                         .values()) {
                hostReadResults.add(remoteTestIteration.getReadsPerSecond());
                hostWriteResults.add(remoteTestIteration.getWritesPerSecond());
                hostDeleteResults.add(remoteTestIteration.getDeletesPerSecond());

                totalReadResults.add(remoteTestIteration.getReadsPerSecond());
                totalWriteResults.add(remoteTestIteration.getWritesPerSecond());
                totalDeleteResults.add(remoteTestIteration.getDeletesPerSecond());
            }

            printResult(hostReadResults, remoteTestResult.getHostName(), "reads");
            printResult(hostWriteResults, remoteTestResult.getHostName(), "writes");
            printResult(hostDeleteResults, remoteTestResult.getHostName(), "deletes");
        }

        printResult(totalReadResults, "test", "reads");
        printResult(totalWriteResults, "test", "writes");
        printResult(totalDeleteResults, "test", "deletes");
    }

    private void printResult(List<Double> results, String owner, String operation) {
        double total = 0;

        for(double d: results)
            total += d;

        double avg = total / results.size();
        System.out.println("Average for " + owner + " for " + operation + ": " + avg);
    }

}