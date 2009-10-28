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

package voldemort.utils.impl;

import java.util.ArrayList;
import java.util.List;

import voldemort.utils.RemoteTestIteration;
import voldemort.utils.RemoteTestResult;

public class RemoteTestSummarizer {

    public void outputTestResults(List<RemoteTestResult> remoteTestResults) {
        List<Double> totalReadResults = new ArrayList<Double>();
        List<Double> totalWriteResults = new ArrayList<Double>();
        List<Double> totalDeleteResults = new ArrayList<Double>();

        for(RemoteTestResult remoteTestResult: remoteTestResults) {
            List<Double> hostReadResults = new ArrayList<Double>();
            List<Double> hostWriteResults = new ArrayList<Double>();
            List<Double> hostDeleteResults = new ArrayList<Double>();

            for(RemoteTestIteration remoteTestIteration: remoteTestResult.getRemoteTestIterations()) {
                hostReadResults.add(remoteTestIteration.getReads());
                hostWriteResults.add(remoteTestIteration.getWrites());
                hostDeleteResults.add(remoteTestIteration.getDeletes());

                totalReadResults.add(remoteTestIteration.getReads());
                totalWriteResults.add(remoteTestIteration.getWrites());
                totalDeleteResults.add(remoteTestIteration.getDeletes());
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
