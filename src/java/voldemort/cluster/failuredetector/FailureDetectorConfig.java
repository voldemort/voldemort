/*
 * Copyright 2009 Mustard Grain, Inc., 2009-2010 LinkedIn, Inc.
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

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import voldemort.client.ClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.utils.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * FailureDetectorConfig simply holds all the data that was available to it upon
 * construction. A FailureDetectorConfig is usually passed to
 * {@link FailureDetectorUtils}'s {@link FailureDetectorUtils#create create}
 * method to create a full-blown {@link FailureDetector} instance.
 * 
 */

public class FailureDetectorConfig {

    public static final String DEFAULT_IMPLEMENTATION_CLASS_NAME = ThresholdFailureDetector.class.getName();

    public static final long DEFAULT_BANNAGE_PERIOD = 30000;

    public static final long DEFAULT_THRESHOLD_INTERVAL = 300000;

    public static final int DEFAULT_THRESHOLD = 95;

    public static final int DEFAULT_THRESHOLD_COUNT_MINIMUM = 30;

    public static final long DEFAULT_ASYNC_RECOVERY_INTERVAL = 10000;

    public static final List<String> DEFAULT_CATASTROPHIC_ERROR_TYPES = ImmutableList.of(ConnectException.class.getName(),
                                                                                         UnknownHostException.class.getName(),
                                                                                         NoRouteToHostException.class.getName());

    public static final long DEFAULT_REQUEST_LENGTH_THRESHOLD = 5000;

    protected String implementationClassName = DEFAULT_IMPLEMENTATION_CLASS_NAME;

    protected long bannagePeriod = DEFAULT_BANNAGE_PERIOD;

    protected int threshold = DEFAULT_THRESHOLD;

    protected int thresholdCountMinimum = DEFAULT_THRESHOLD_COUNT_MINIMUM;

    protected long thresholdInterval = DEFAULT_THRESHOLD_INTERVAL;

    protected long asyncRecoveryInterval = DEFAULT_ASYNC_RECOVERY_INTERVAL;

    protected List<String> catastrophicErrorTypes = DEFAULT_CATASTROPHIC_ERROR_TYPES;

    protected long requestLengthThreshold = DEFAULT_REQUEST_LENGTH_THRESHOLD;

    protected Collection<Node> nodes;

    protected StoreVerifier storeVerifier;

    protected Time time = SystemTime.INSTANCE;

    private Cluster cluster = null;

    /**
     * Constructs a new FailureDetectorConfig using all the defaults. This is
     * usually used in the case of unit tests.
     * 
     * <p/>
     * 
     * <b>Note</b>: the {@link #setNodes(Collection)} and
     * {@link #setStoreVerifier(StoreVerifier)} methods must be called to ensure
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
     * {@link #setStoreVerifier(StoreVerifier)} methods must be called to ensure
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
        setCatastrophicErrorTypes(config.getFailureDetectorCatastrophicErrorTypes());
        setRequestLengthThreshold(config.getFailureDetectorRequestLengthThreshold());
    }

    /**
     * Constructs a new FailureDetectorConfig from a client perspective (via
     * {@link ClientConfig}).
     * 
     * <p/>
     * 
     * <b>Note</b>: the {@link #setNodes(Collection)} and
     * {@link #setStoreVerifier(StoreVerifier)} methods must be called to ensure
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
        setCatastrophicErrorTypes(config.getFailureDetectorCatastrophicErrorTypes());
        setRequestLengthThreshold(config.getFailureDetectorRequestLengthThreshold());
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
     * Returns the minimum number of failures that must occur before the success
     * ratio is calculated to compare against the success threshold percentage.
     * 
     * <p/>
     * 
     * <b>Note</b>: this is only used by the {@link ThresholdFailureDetector}
     * implementation.
     * 
     * @return Integer representing the minimum number of failures (per node)
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
     * Assigns the minimum number of failures that must occur before the success
     * ratio is calculated to compare against the success threshold percentage.
     * 
     * <p/>
     * 
     * <b>Note</b>: this is only used by the {@link ThresholdFailureDetector}
     * implementation.
     * 
     * @param thresholdCountMinimum Integer representing the minimum number of
     *        failures (per node) that must be processed before the threshold is
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
        if(thresholdCountMinimum < 0)
            throw new IllegalArgumentException("thresholdCountMinimum must be greater than or equal to 0");

        this.thresholdCountMinimum = thresholdCountMinimum;
        return this;
    }

    /**
     * Returns the interval of time for each the success ratio is valid. After
     * this number of milliseconds passes, a new interval is started and all
     * internal state of a node is cleared out. However, it may not necessarily
     * be marked as available if it was unavailable in the previous interval.
     * 
     * <p/>
     * 
     * <b>Note</b>: this is only used by the {@link ThresholdFailureDetector}
     * implementation.
     * 
     * @return Millisecond interval for the success ratio
     * 
     * @see ThresholdFailureDetector
     * @see VoldemortConfig#getFailureDetectorThresholdInterval
     * @see ClientConfig#getFailureDetectorThresholdInterval
     */

    public long getThresholdInterval() {
        return thresholdInterval;
    }

    /**
     * Assigns the interval of time for each the success ratio is valid. After
     * this number of milliseconds passes, a new interval is started and all
     * internal state of a node is cleared out. However, it may not necessarily
     * be marked as available if it was unavailable in the previous interval.
     * 
     * <p/>
     * 
     * <b>Note</b>: this is only used by the {@link ThresholdFailureDetector}
     * implementation.
     * 
     * @param thresholdInterval Millisecond interval for the success ratio
     * 
     * @exception IllegalArgumentException Thrown if the thresholdInterval
     *            parameter is less than or equal to 0
     * 
     * @see ThresholdFailureDetector
     * @see VoldemortConfig#getFailureDetectorThresholdInterval
     * @see ClientConfig#getFailureDetectorThresholdInterval
     */

    public FailureDetectorConfig setThresholdInterval(long thresholdInterval) {
        if(thresholdInterval <= 0)
            throw new IllegalArgumentException("thresholdInterval must be greater than 0");

        this.thresholdInterval = thresholdInterval;
        return this;
    }

    /**
     * Returns the interval of time (in milliseconds) that the thread will wait
     * before checking if a given node has recovered.
     * 
     * <p/>
     * 
     * <b>Note</b>: this is only used by the
     * {@link AsyncRecoveryFailureDetector} and {@link ThresholdFailureDetector}
     * implementations.
     * 
     * @return Integer representing the millisecond interval for the success
     *         ratio
     * 
     * @see AsyncRecoveryFailureDetector
     * @see ThresholdFailureDetector
     * @see VoldemortConfig#getFailureDetectorAsyncRecoveryInterval
     * @see ClientConfig#getFailureDetectorAsyncRecoveryInterval
     */

    public long getAsyncRecoveryInterval() {
        return asyncRecoveryInterval;
    }

    /**
     * Assigns the interval of time (in milliseconds) that the thread will wait
     * before checking if a given node has recovered.
     * 
     * <p/>
     * 
     * <b>Note</b>: this is only used by the
     * {@link AsyncRecoveryFailureDetector} and {@link ThresholdFailureDetector}
     * implementations.
     * 
     * @param asyncRecoveryInterval Number of milliseconds to wait between
     *        recovery attempts
     * 
     * @exception IllegalArgumentException Thrown if the thresholdInterval
     *            parameter is less than or equal to 0
     * 
     * @see AsyncRecoveryFailureDetector
     * @see ThresholdFailureDetector
     * @see VoldemortConfig#getFailureDetectorAsyncRecoveryInterval
     * @see ClientConfig#getFailureDetectorAsyncRecoveryInterval
     */

    public FailureDetectorConfig setAsyncRecoveryInterval(long asyncRecoveryInterval) {
        if(asyncRecoveryInterval <= 0)
            throw new IllegalArgumentException("asyncRecoveryInterval must be greater than 0");

        this.asyncRecoveryInterval = asyncRecoveryInterval;
        return this;
    }

    /**
     * Returns the list of Java Exception types that are considered
     * catastrophic. Some FailureDetector implementations may not mark a given
     * node as unavailable on each and every call to recordException. Instead
     * they may apply logic to determine if such an exception should cause the
     * node to be marked as unavailable. However, the list of so-called
     * catastrophic errors provides such FailureDetector implementations a hint
     * that receipt of such errors should cause the node to be marked as
     * unavailable immediately, regardless of other logic.
     * 
     * <p/>
     * 
     * <b>Note</b>: this is only used by the {@link ThresholdFailureDetector}
     * implementation.
     * 
     * @return List of fully-qualified Java Exception class names against which
     *         to check the exception provided to recordException; this list
     *         should be immutable
     * 
     * @see ThresholdFailureDetector
     * @see VoldemortConfig#getFailureDetectorCatastrophicErrorTypes
     * @see ClientConfig#getFailureDetectorCatastrophicErrorTypes
     */

    public List<String> getCatastrophicErrorTypes() {
        return catastrophicErrorTypes;
    }

    /**
     * Assigns the list of Java Exception types that are considered
     * catastrophic. Some FailureDetector implementations may not mark a given
     * node as unavailable on each and every call to recordException. Instead
     * they may apply logic to determine if such an exception should cause the
     * node to be marked as unavailable. However, the list of so-called
     * catastrophic errors provides such FailureDetector implementations a hint
     * that receipt of such errors should cause the node to be marked as
     * unavailable immediately, regardless of other logic.
     * 
     * <p/>
     * 
     * <b>Note</b>: this is only used by the {@link ThresholdFailureDetector}
     * implementation.
     * 
     * @param catastrophicErrorTypes List of fully-qualified Java Exception
     *        class names against which to check the exception provided to
     *        recordException; this list should be immutable and non-null
     * 
     * @see ThresholdFailureDetector
     * @see VoldemortConfig#getFailureDetectorCatastrophicErrorTypes
     * @see ClientConfig#getFailureDetectorCatastrophicErrorTypes
     */

    public FailureDetectorConfig setCatastrophicErrorTypes(List<String> catastrophicErrorTypes) {
        this.catastrophicErrorTypes = Utils.notNull(catastrophicErrorTypes);
        return this;
    }

    /**
     * Returns the maximum time (in milliseconds) that a request (get, put,
     * delete, etc.) can take before a given <i>successful</i> event is
     * considered as a failure because the requests are--while
     * successful--considered to be taking too long to complete.
     * 
     * <p/>
     * 
     * <b>Note</b>: this is only used by the {@link ThresholdFailureDetector}
     * implementation.
     * 
     * @return Number of milliseconds representing maximum amount of time the
     *         request should take before being considered as a failure
     * 
     * @see ThresholdFailureDetector
     * @see VoldemortConfig#getFailureDetectorRequestLengthThreshold
     * @see ClientConfig#getFailureDetectorRequestLengthThreshold
     */

    public long getRequestLengthThreshold() {
        return requestLengthThreshold;
    }

    /**
     * Assigns the value for the maximum time (in milliseconds) that a request
     * (get, put, delete, etc.) can take before a given <i>successful</i> event
     * is considered as a failure because the requests are--while
     * successful--considered to be taking too long to complete.
     * 
     * <p/>
     * 
     * <b>Note</b>: this is only used by the {@link ThresholdFailureDetector}
     * implementation.
     * 
     * @param requestLengthThreshold Number of milliseconds representing maximum
     *        amount of time the request should take before being considered as
     *        a failure
     * 
     * @exception IllegalArgumentException Thrown if the requestLengthThreshold
     *            parameter is less than 0
     * 
     * @see ThresholdFailureDetector
     * @see VoldemortConfig#getFailureDetectorRequestLengthThreshold
     * @see ClientConfig#getFailureDetectorRequestLengthThreshold
     */

    public FailureDetectorConfig setRequestLengthThreshold(long requestLengthThreshold) {
        if(requestLengthThreshold <= 0)
            throw new IllegalArgumentException("requestLengthThreshold must be positive");

        this.requestLengthThreshold = requestLengthThreshold;
        return this;
    }

    /**
     * Returns a reference to the cluster object
     * 
     * @return Cluster object which determines the source of truth for the
     *         topology
     */

    public Cluster getCluster() {
        return this.cluster;
    }

    /**
     * Assigns a cluster which determines the source of truth for the topology
     * 
     * @param cluster The Cluster object retrieved during bootstrap; must be
     *        non-null
     */

    public FailureDetectorConfig setCluster(Cluster cluster) {
        Utils.notNull(cluster);
        this.cluster = cluster;
        return this;
    }

    /**
     * Returns a list of nodes in the cluster represented by this failure
     * detector configuration.
     * 
     * @return Collection of Node instances, usually determined from the Cluster
     */

    @Deprecated
    public synchronized Collection<Node> getNodes() {
        return ImmutableSet.copyOf(this.cluster.getNodes());
    }

    /**
     * Assigns a list of nodes in the cluster represented by this failure
     * detector configuration.
     * 
     * @param nodes Collection of Node instances, usually determined from the
     *        Cluster; must be non-null
     */

    @Deprecated
    public synchronized FailureDetectorConfig setNodes(Collection<Node> nodes) {
        Utils.notNull(nodes);
        this.nodes = new HashSet<Node>(nodes);
        return this;
    }

    public synchronized void addNode(Node node) {
        Utils.notNull(node);
        nodes.add(node);
    }

    public synchronized void removeNode(Node node) {
        Utils.notNull(node);
        nodes.remove(node);
    }

    public StoreVerifier getStoreVerifier() {
        return storeVerifier;
    }

    public FailureDetectorConfig setStoreVerifier(StoreVerifier storeVerifier) {
        this.storeVerifier = Utils.notNull(storeVerifier);
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
