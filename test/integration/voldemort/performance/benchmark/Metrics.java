/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
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
 * the License. See accompanying LICENSE file.
 */

package voldemort.performance.benchmark;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import voldemort.utils.Props;

public class Metrics {

    private static Metrics singleton = null;
    private static Props metricProps = null;

    private Map<String, Measurement> data;
    private boolean summaryOnly = false;

    public static void setProperties(Props props) {
        metricProps = props;
    }

    public synchronized static Metrics getInstance() {
        if(singleton == null) {
            singleton = new Metrics();
        }
        return singleton;
    }

    private Metrics() {
        this.data = new ConcurrentHashMap<String, Measurement>();

        String metricType = Metrics.metricProps.getString(Benchmark.METRIC_TYPE,
                                                          Benchmark.SUMMARY_METRIC_TYPE);
        if(metricType.compareTo(Benchmark.SUMMARY_METRIC_TYPE) == 0) {
            this.summaryOnly = true;
        } else {
            this.summaryOnly = false;
        }
    }

    private Measurement constructMeasurement(String name) {
        return new Measurement(name, this.summaryOnly);
    }

    public synchronized void recordLatency(String operation, int latency) {
        if(!data.containsKey(operation)) {
            synchronized(this) {
                if(!data.containsKey(operation)) {
                    data.put(operation, constructMeasurement(operation));
                }
            }
        }
        data.get(operation).recordLatency(latency);
    }

    public void recordReturnCode(String operation, int code) {
        if(!data.containsKey(operation)) {
            synchronized(this) {
                if(!data.containsKey(operation)) {
                    data.put(operation, constructMeasurement(operation));
                }
            }
        }
        data.get(operation).recordReturnCode(code);
    }

    public void reset() {
        data.clear();
    }

    public void printReport(PrintStream out) {
        for(Measurement m: data.values()) {
            m.printReport(out);
        }
    }

    public HashMap<String, Results> getResults() {
        HashMap<String, Results> returnMap = new HashMap<String, Results>();
        for(Measurement m: data.values()) {
            returnMap.put(m.getName(), m.generateResults());
        }
        return returnMap;
    }

}
