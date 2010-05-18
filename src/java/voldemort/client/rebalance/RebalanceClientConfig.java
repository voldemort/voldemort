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

    private int maxParallelDonors = 1;
    private int maxParallelRebalancing = 1;
    private int rebalancingClientTimeoutSeconds = 7 * 24 * 60 * 60;
    private boolean deleteAfterRebalancingEnabled;

    public static final String MaxParallelRebalancingString = "max.parallel.rebalancing";
    public static final String RebalancingClientTimeoutSeconds = "rebalancing.client.timeout.seconds";
    public static final String EnableDeleteAfterRebalancing = "enable.delete.after.rebalancing";
    public static final String MaxParallelDonorsString = "max.parallel.donors";

    public RebalanceClientConfig(Properties properties) {
        super(properties);
        Props props = new Props(properties);

        if (props.containsKey(MaxParallelDonorsString))
            this.setMaxParallelDonors(props.getInt(MaxParallelDonorsString));
        
        if(props.containsKey(MaxParallelRebalancingString))
            this.setMaxParallelRebalancing(props.getInt(MaxParallelRebalancingString));

        if(props.containsKey(RebalancingClientTimeoutSeconds))
            this.setRebalancingClientTimeoutSeconds(props.getInt(RebalancingClientTimeoutSeconds));

        if(props.containsKey(EnableDeleteAfterRebalancing))
            this.setDeleteAfterRebalancingEnabled(props.getBoolean(EnableDeleteAfterRebalancing));

    }

    public RebalanceClientConfig() {
        this(new Properties());
    }

    public void setMaxParallelDonors(int maxParallelDonors) {
        this.maxParallelDonors = maxParallelDonors;
    }

    public int getMaxParallelDonors() {
        return maxParallelDonors;
    }

    public void setMaxParallelRebalancing(int maxParallelRebalancing) {
        this.maxParallelRebalancing = maxParallelRebalancing;
    }

    public int getMaxParallelRebalancing() {
        return maxParallelRebalancing;
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

}
