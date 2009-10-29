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

import java.util.Map;

import org.apache.commons.logging.Log;

import voldemort.utils.RemoteTestResult;
import voldemort.utils.RemoteTestResult.RemoteTestIteration;

public class RemoteTestOutputParser implements CommandOutputListener {

    private final static String ITERATOR_LINE_TAG = " iteration = ";

    private final static String ITERATIONS_TOTAL_LINE_TAG = "iterations : ";

    private final static String THROUGHPUT_LINE_TAG = "Throughput: ";

    private final Log log;

    private final RemoteTestResult remoteTestResult;

    private int iterationIndex;

    private int totalIterations;

    public RemoteTestOutputParser(Log log, RemoteTestResult remoteTestResult) {
        this.log = log;
        this.remoteTestResult = remoteTestResult;
    }

    public void outputReceived(String hostName, String line) {
        int i = line.indexOf(ITERATOR_LINE_TAG);

        if(i != -1) {
            iterationIndex = Integer.parseInt(line.substring(i + ITERATOR_LINE_TAG.length())
                                                  .replace("=", "")
                                                  .trim());

            if(log.isInfoEnabled()) {
                int percentComplete = totalIterations != 0 ? (iterationIndex * 100)
                                                             / totalIterations : -1;

                log.info(hostName + " starting iteration " + iterationIndex
                         + ((percentComplete != -1) ? " " + percentComplete + "%" : ""));
            }

        } else if(line.startsWith(THROUGHPUT_LINE_TAG)) {
            Map<Integer, RemoteTestIteration> map = remoteTestResult.getRemoteTestIterations();

            RemoteTestIteration remoteTestIteration = map.get(iterationIndex);

            if(remoteTestIteration == null) {
                remoteTestIteration = new RemoteTestIteration();
                map.put(iterationIndex, remoteTestIteration);
            }

            double value = Double.parseDouble(line.substring(THROUGHPUT_LINE_TAG.length(),
                                                             line.indexOf(" ",
                                                                          THROUGHPUT_LINE_TAG.length())));

            if(line.contains("writes"))
                remoteTestIteration.setWritesPerSecond(value);
            else if(line.contains("reads"))
                remoteTestIteration.setReadsPerSecond(value);
            else if(line.contains("deletes"))
                remoteTestIteration.setDeletesPerSecond(value);
        } else if((i = line.indexOf(ITERATIONS_TOTAL_LINE_TAG)) != -1) {
            totalIterations = Integer.parseInt(line.substring(i
                                                              + ITERATIONS_TOTAL_LINE_TAG.length())
                                                   .replace("=", "")
                                                   .trim());
        }
    }

}