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

import java.util.SortedMap;
import java.util.TreeMap;

/**
 * RemoteTestResult is the output from the RemoteTest operation. For the given
 * client system on which the test was run, we capture the host name as well as
 * a mapping of the values for the reads/second, writes/second, and
 * deletes/second for each iteration. These raw numbers can then be manipulated
 * as seen fit for the desired measurement.
 * 
 * @author Kirk True
 */

public class RemoteTestResult {

    private final String hostName;

    private final SortedMap<Integer, RemoteTestIteration> remoteTestIterations;

    public RemoteTestResult(String hostName) {
        this.hostName = hostName;
        this.remoteTestIterations = new TreeMap<Integer, RemoteTestIteration>();
    }

    public String getHostName() {
        return hostName;
    }

    /**
     * Note: we return the actual object reference here - not a copy. At
     * present, some implementations may intentionally directly alter this as
     * the test is in progress.
     * 
     * @return Map sorted and keyed by its iteration index with the value being
     *         the RemoteTestIteration instance.
     */

    public SortedMap<Integer, RemoteTestIteration> getRemoteTestIterations() {
        return remoteTestIterations;
    }

    public static class RemoteTestIteration {

        private double readsPerSecond;

        private double writesPerSecond;

        private double deletesPerSecond;

        public double getReadsPerSecond() {
            return readsPerSecond;
        }

        public void setReadsPerSecond(double readsPerSecond) {
            this.readsPerSecond = readsPerSecond;
        }

        public double getWritesPerSecond() {
            return writesPerSecond;
        }

        public void setWritesPerSecond(double writesPerSecond) {
            this.writesPerSecond = writesPerSecond;
        }

        public double getDeletesPerSecond() {
            return deletesPerSecond;
        }

        public void setDeletesPerSecond(double deletesPerSecond) {
            this.deletesPerSecond = deletesPerSecond;
        }

    }

}
