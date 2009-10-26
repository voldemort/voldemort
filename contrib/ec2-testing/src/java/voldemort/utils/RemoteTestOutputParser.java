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

package voldemort.utils;

import java.util.HashMap;
import java.util.Map;

public class RemoteTestOutputParser {

    private final static String ITERATOR_LINE_TAG = " iteration = ";

    private final static String THROUGHPUT_LINE_TAG = "Throughput: ";

    private final Map<Integer, RemoteTestIteration> remoteTestIterations;

    private int iterationIndex;

    public RemoteTestOutputParser() {
        this.remoteTestIterations = new HashMap<Integer, RemoteTestIteration>();
    }

    public Map<Integer, RemoteTestIteration> getRemoteTestIterations() {
        return remoteTestIterations;
    }

    public void outputReceived(String line) {
        int i = line.indexOf(ITERATOR_LINE_TAG);

        if(i != -1) {
            iterationIndex = Integer.parseInt(line.substring(i + ITERATOR_LINE_TAG.length())
                                                  .replace("=", "")
                                                  .trim());
        } else if(line.startsWith(THROUGHPUT_LINE_TAG)) {
            RemoteTestIteration remoteTestIteration = remoteTestIterations.get(iterationIndex);

            if(remoteTestIteration == null) {
                remoteTestIteration = new RemoteTestIteration();
                remoteTestIterations.put(iterationIndex, remoteTestIteration);
            }

            double value = Double.parseDouble(line.substring(THROUGHPUT_LINE_TAG.length(),
                                                             line.indexOf(" ",
                                                                          THROUGHPUT_LINE_TAG.length())));

            if(line.contains("writes"))
                remoteTestIteration.setWrites(value);
            else if(line.contains("reads"))
                remoteTestIteration.setReads(value);
            else if(line.contains("deletes"))
                remoteTestIteration.setDeletes(value);
        }
    }
}