/*
 * Copyright 2009 Mustard Grain, Inc.
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

package voldemort.cluster.failuredetector;

import java.util.Collection;

import voldemort.client.ClientConfig;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.utils.Utils;

/**
 * FailureDetectorConfig simply holds all the data that was available to it upon
 * construction. A FailureDetectorConfig is usually passed to
 * {@link FailureDetectorUtils}'s {@link FailureDetectorUtils#create create}
 * method to create a full-blown {@link FailureDetector} instance.
 * 
 * @author Kirk True
 */

public class FailureDetectorConfig {

    public static final String DEFAULT_IMPLEMENTATION_CLASS_NAME = ThresholdFailureDetector.class.getName();

    public static final long DEFAULT_BANNAGE_PERIOD = 30000;

    public static final long DEFAULT_THRESHOLD_INTERVAL = 10000;

    public static final int DEFAULT_THRESHOLD = 80;

    public static final int DEFAULT_THRESHOLD_COUNT_MINIMUM = 10;

    public static final long DEFAULT_ASYNC_RECOVERY_INTERVAL = 10000;

    public static final boolean DEFAULT_IS_JMX_ENABLED = false;

    protected String implementationClassName = DEFAULT_IMPLEMENTATION_CLASS_NAME;

    protected long bannagePeriod = DEFAULT_BANNAGE_PERIOD;

    protected int threshold = DEFAULT_THRESHOLD;

    protected int thresholdCountMinimum = DEFAULT_THRESHOLD_COUNT_MINIMUM;

    protected long thresholdInterval = DEFAULT_THRESHOLD_INTERVAL;

    protected long asyncRecoveryInterval = DEFAULT_ASYNC_RECOVERY_INTERVAL;

    protected boolean isJmxEnabled = DEFAULT_IS_JMX_ENABLED;

    protected Collection<Node> nodes;

    protected StoreResolver storeResolver;

    protected Time time = SystemTime.INSTANCE;

    /**
     * Constructs a new FailureDetectorConfig using all the defaults. This is
     * usually used in the case of unit tests.
     * 
     * <p/>
     * 
     * <b>Note</b>: the {@link #setNodes(Collection)} and
     * {@link #setStoreResolver(StoreResolver)} methods must be called to ensure
     * <i>complete</i> configuration.
     */

    public FailureDetectorConfig() {

    }

    /**
     * Constructs a new FailureDetectorConfig from a server perspective (via
     * {@link VoldemortConfig}).
     * 
     * <p/>
     * 
     * <b>Note</b>: the {@link #setNodes(Collection)} and
     * {@link #setStoreResolver(StoreResolver)} methods must be called to ensure
     * <i>complete</i> configuration.
     * 
     * @param config {@link VoldemortConfig} instance
     */

    public FailureDetectorConfig(VoldemortConfig config) {
        setImplementationClassName(config.getFailureDetectorImplementation());
        setBannagePeriod(config.getFailureDetectorBannagePeriod());
        setThreshold(config.getFailureDetectorThreshold());
        setThresholdCountMinimum(config.getFailureDetectorThresholdCountMinimum());
        setThresholdInterval(config.getFailureDetectorThresholdInterval());
        setAsyncRecoveryInterval(config.getFailureDetectorAsyncRecoveryInterval());
        setJmxEnabled(config.isJmxEnabled());
    }

    /**
     * Constructs a new FailureDetectorConfig from a client perspective (via
     * {@link ClientConfig}).
     * 
     * <p/>
     * 
     * <b>Note</b>: the {@link #setNodes(Collection)} and
     * {@link #setStoreResolver(StoreResolver)} methods must be called to ensure
     * <i>complete</i> configuration.
     * 
     * @param config {@link ClientConfig} instance
     */

    public FailureDetectorConfig(ClientConfig config) {
        setImplementationClassName(config.getFailureDetectorImplementation());
        setBannagePeriod(config.getFailureDetectorBannagePeriod());
        setThreshold(config.getFailureDetectorThreshold());
        setThresholdCountMinimum(config.getFailureDetectorThresholdCountMinimum());
        setThresholdInterval(config.getFailureDetectorThresholdInterval());
        setAsyncRecoveryInterval(config.getFailureDetectorAsyncRecoveryInterval());
        setJmxEnabled(config.isJmxEnabled());
    }

    /**
     * Returns the fully-qualified class name of the FailureDetector
     * implementation.
     * 
     * @return Class name to instantiate for the FailureDetector
     * 
     * @see VoldemortConfig#getFailureDetectorImplementation
     * @see ClientConfig#getFailureDetectorImplementation
     */

    public String getImplementationClassName() {
        return implementationClassName;
    }

    /**
     * Assigns the fully-qualified class name of the FailureDetector
     * implementation.
     * 
     * @param implementationClassName Class name to instantiate for the
     *        FailureDetector
     * 
     * @see VoldemortConfig#getFailureDetectorImplementation
     * @see ClientConfig#getFailureDetectorImplementation
     */

    public FailureDetectorConfig setImplementationClassName(String implementationClassName) {
        this.implementationClassName = Utils.notNull(implementationClassName);
        return this;
    }

    /**
     * Returns the node bannage period (in milliseconds) as defined by the
     * client or server configuration. Some FailureDetector implementations wait
     * for a specified period of time before attempting to access the node again
     * once it has become unavailable.
     * 
     * <p/>
     * 
     * <b>Note</b>: this is only used by the
     * {@link BannagePeriodFailureDetector} implementation.
     * 
     * @return Period of bannage of a node, in milliseconds
     * 
     * @see BannagePeriodFailureDetector
     * @see VoldemortConfig#getFailureDetectorBannagePeriod
     * @see ClientConfig#getFailureDetectorBannagePeriod
     */

    public long getBannagePeriod() {
        return bannagePeriod;
    }

    /**
     * Assigns the node bannage period (in milliseconds) as defined by the
     * client or server configuration. Some FailureDetector implementations wait
     * for a specified period of time before attempting to access the node again
     * once it has become unavailable.
     * 
     * <p/>
     * 
     * <b>Note</b>: this is only used by the
     * {@link BannagePeriodFailureDetector} implementation.
     * 
     * @param bannagePeriod Period of bannage of a node, in milliseconds
     * 
     * @see BannagePeriodFailureDetector
     * @see VoldemortConfig#getFailureDetectorBannagePeriod
     * @see ClientConfig#getFailureDetectorBannagePeriod
     */

    public FailureDetectorConfig setBannagePeriod(long bannagePeriod) {
        this.bannagePeriod = bannagePeriod;
        return this;
    }

    /**
     * Returns the success threshold percentage with an integer value between 0
     * and 100. Some FailureDetector implementations will mark a node as
     * unavailable if the ratio of successes to total requests for that node
     * falls under this threshold.
     * 
     * <p/>
     * 
     * <b>Note</b>: this is only used by the {@link ThresholdFailureDetector}
     * implementation.
     * 
     * @return Integer percentage representing success threshold
     * 
     * @see ThresholdFailureDetector
     * @see VoldemortConfig#getFailureDetectorThreshold
     * @see ClientConfig#getFailureDetectorThreshold
     */

    public int getThreshold() {
        return threshold;
    }

    /**
     * Assigns the success threshold percentage with an integer value between 0
     * and 100. Some FailureDetector implementations will mark a node as
     * unavailable if the ratio of successes to total requests for that node
     * falls under this threshold.
     * 
     * <p/>
     * 
     * <b>Note</b>: this is only used by the {@link ThresholdFailureDetector}
     * implementation.
     * 
     * @param threshold Integer percentage representing success threshold
     * 
     * @exception IllegalArgumentException Thrown if the threshold parameter is
     *            outside the range [0..100]
     * 
     * @see ThresholdFailureDetector
     * @see VoldemortConfig#getFailureDetectorThreshold
     * @see ClientConfig#getFailureDetectorThreshold
     */

    public FailureDetectorConfig setThreshold(int threshold) {
        if(threshold < 0 || threshold > 100)
            throw new IllegalArgumentException("threshold must be in the range (0..100)");

        this.threshold = threshold;
        return this;
    }

    /**
     * Returns the minimum number of requests that must occur before the success
     * ratio is calculated to compare against the success threshold percentage.
     * 
     * <p/>
     * 
     * <b>Note</b>: this is only used by the {@link ThresholdFailureDetector}
     * implementation.
     * 
     * @return Integer representing the minimum number of requests (per node)
     *         that must be processed before the threshold is checked
     * 
     * @see ThresholdFailureDetector
     * @see VoldemortConfig#getFailureDetectorThreshold
     * @see ClientConfig#getFailureDetectorThreshold
     */

    public int getThresholdCountMinimum() {
        return thresholdCountMinimum;
    }

    /**
     * Assigns the minimum number of requests that must occur before the success
     * ratio is calculated to compare against the success threshold percentage.
     * 
     * <p/>
     * 
     * <b>Note</b>: this is only used by the {@link ThresholdFailureDetector}
     * implementation.
     * 
     * @param thresholdCountMinimum Integer representing the minimum number of
     *        requests (per node) that must be processed before the threshold is
     *        checked
     * 
     * @exception IllegalArgumentException Thrown if the thresholdCountMinimum
     *            parameter is outside the range [0..Integer.MAX_VALUE]
     * 
     * @see ThresholdFailureDetector
     * @see VoldemortConfig#getFailureDetectorThreshold
     * @see ClientConfig#getFailureDetectorThreshold
     */

    public FailureDetectorConfig setThresholdCountMinimum(int thresholdCountMinimum) {
        if(threshold < 0)
            throw new IllegalArgumentException("thresholdCountMinimum must be greater than or equal to 0");

        this.thresholdCountMinimum = thresholdCountMinimum;
        return this;
    }

    public long getThresholdInterval() {
        return thresholdInterval;
    }

    public FailureDetectorConfig setThresholdInterval(long thresholdInterval) {
        this.thresholdInterval = thresholdInterval;
        return this;
    }

    public long getAsyncRecoveryInterval() {
        return asyncRecoveryInterval;
    }

    public FailureDetectorConfig setAsyncRecoveryInterval(long asyncRecoveryInterval) {
        this.asyncRecoveryInterval = asyncRecoveryInterval;
        return this;
    }

    public boolean isJmxEnabled() {
        return isJmxEnabled;
    }

    public FailureDetectorConfig setJmxEnabled(boolean isJmxEnabled) {
        this.isJmxEnabled = isJmxEnabled;
        return this;
    }

    /**
     * Returns a list of nodes in the cluster represented by this failure
     * detector configuration.
     * 
     * @return Collection of Node instances, usually determined from the Cluster
     */

    public Collection<Node> getNodes() {
        return nodes;
    }

    /**
     * Assigns a list of nodes in the cluster represented by this failure
     * detector configuration.
     * 
     * @param nodes Collection of Node instances, usually determined from the
     *        Cluster; must be non-null
     */

    public FailureDetectorConfig setNodes(Collection<Node> nodes) {
        this.nodes = Utils.notNull(nodes);
        return this;
    }

    public StoreResolver getStoreResolver() {
        return storeResolver;
    }

    public FailureDetectorConfig setStoreResolver(StoreResolver storeResolver) {
        this.storeResolver = Utils.notNull(storeResolver);
        return this;
    }

    public Time getTime() {
        return time;
    }

    public FailureDetectorConfig setTime(Time time) {
        this.time = Utils.notNull(time);
        return this;
    }

}
