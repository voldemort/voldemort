/*
 * Copyright 2008-2010 LinkedIn, Inc
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

package voldemort.client.rebalance;

import java.util.Properties;

import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.utils.Props;

public class RebalanceClientConfig extends AdminClientConfig {

    public final static int MAX_PARALLEL_REBALANCING = 1;
    public final static int MAX_TRIES = 2;

    private int maxParallelRebalancing = MAX_PARALLEL_REBALANCING;
    private int maxTriesRebalancing = MAX_TRIES;
    private int rebalancingClientTimeoutSeconds = 7 * 24 * 60 * 60;
    private boolean deleteAfterRebalancingEnabled;
    private boolean enabledShowPlan;
    private String outputDirectory = null;

    public static final String MaxParallelRebalancingString = "max.parallel.rebalancing";
    public static final String RebalancingClientTimeoutSeconds = "rebalancing.client.timeout.seconds";
    public static final String EnableDeleteAfterRebalancing = "enable.delete.after.rebalancing";
    public static final String MaxTriesRebalancingString = "max.tries.rebalancing";
    public static final String OutputDirectoryString = "rebalancing.output.dir";

    public RebalanceClientConfig(Properties properties) {
        super(properties);
        Props props = new Props(properties);

        if(props.containsKey(MaxParallelRebalancingString))
            this.setMaxParallelRebalancing(props.getInt(MaxParallelRebalancingString));

        if(props.containsKey(RebalancingClientTimeoutSeconds))
            this.setRebalancingClientTimeoutSeconds(props.getInt(RebalancingClientTimeoutSeconds));

        if(props.containsKey(EnableDeleteAfterRebalancing))
            this.setDeleteAfterRebalancingEnabled(props.getBoolean(EnableDeleteAfterRebalancing));

        if(props.containsKey(MaxTriesRebalancingString))
            this.setMaxTriesRebalancing(props.getInt(MaxTriesRebalancingString));

        if(props.containsKey(OutputDirectoryString))
            this.setOutputDirectory(props.getString(OutputDirectoryString));

    }

    public RebalanceClientConfig() {
        this(new Properties());
    }

    public void setOutputDirectory(String outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    public String getOutputDirectory() {
        return this.outputDirectory;
    }

    public boolean hasOutputDirectory() {
        return this.outputDirectory != null;
    }

    public void setMaxParallelRebalancing(int maxParallelRebalancing) {
        this.maxParallelRebalancing = maxParallelRebalancing;
    }

    public int getMaxParallelRebalancing() {
        return maxParallelRebalancing;
    }

    public void setMaxTriesRebalancing(int maxTriesRebalancing) {
        this.maxTriesRebalancing = maxTriesRebalancing;
    }

    public int getMaxTriesRebalancing() {
        return maxTriesRebalancing;
    }

    public void setRebalancingClientTimeoutSeconds(int rebalancingTimeoutSeconds) {
        this.rebalancingClientTimeoutSeconds = rebalancingTimeoutSeconds;
    }

    public int getRebalancingClientTimeoutSeconds() {
        return rebalancingClientTimeoutSeconds;
    }

    public boolean isDeleteAfterRebalancingEnabled() {
        return deleteAfterRebalancingEnabled;
    }

    public void setDeleteAfterRebalancingEnabled(boolean deleteAfterRebalancingEnabled) {
        this.deleteAfterRebalancingEnabled = deleteAfterRebalancingEnabled;
    }

    public void setEnableShowPlan(boolean enable) {
        this.enabledShowPlan = enable;
    }

    public boolean isShowPlanEnabled() {
        return enabledShowPlan;
    }
}
