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

import voldemort.cluster.Node;
import voldemort.store.routed.RoutedStore;

/**
 * The FailureDetector API is used to determine a cluster's node availability.
 * Machines and servers can go down at any time and usage of this API can be
 * used by request routing in an attempt to avoid unavailable servers.
 * 
 * <p/>
 * 
 * A FailureDetector is specific to a given cluster and as such there should
 * only be one instance per cluster per JVM.
 * 
 * <p/>
 * 
 * Implementations can differ dramatically in how they approach the problem of
 * determining node availability. Some implementations may rely heavily on
 * invocations of recordException and recordSuccess to determine availability.
 * The result is that such a FailureDetector implementation performs little
 * logic other than bookkeeping, implicitly trusting users of the API. However,
 * other implementations may be more selective in using results of any external
 * users' calls to the recordException and recordSuccess methods.
 * Implementations may use these error/success calls as "hints" or may ignore
 * them outright.
 * 
 * <p/>
 * 
 * To contrast the two approaches to implementing:
 * 
 * <ol>
 * <li><b>Externally-based implementations</b> use algorithms that rely heavily
 * on users for correctness. For example, let's say a user attempts to contact a
 * node which then fails. A responsible caller should invoke the recordException
 * API to inform the FailureDetector that an error has taken place for the node.
 * The FailureDetector itself hasn't really determined availability itself. So
 * if the caller is incorrect or buggy, the FailureDetector's accuracy is
 * compromised.</li>
 * <li><b>Internally-based implementations</b> rely on their own determination
 * of node availability. For example, a heartbeat style implementation may pay
 * only a modicum of attention when its recordException and/or recordSuccess
 * methods are invoked by outside callers.</li>
 * </ol>
 * 
 * Naturally there is a spectrum of implementations and external calls to
 * recordException and recordSuccess should (not must) provide some input to the
 * internal algorithm.
 * 
 * @author jay
 * @author Kirk True
 * 
 * @see RoutedStore
 */

public interface FailureDetector {

    /**
     * Determines if the node is available or offline.
     * 
     * The isAvailable method is a simple boolean operation to determine if the
     * node in question is available. As expected, the result of this call is an
     * approximation given race conditions. However, the FailureDetector should
     * do its best to determine the then-current state of the cluster to produce
     * a minimum of false negatives and false positives.
     * 
     * <p/>
     * 
     * <b>Note</b>: this determination is approximate and differs based upon the
     * algorithm used by the implementation.
     * 
     * @param node Node to check
     * 
     * @return True if available, false otherwise
     */

    public boolean isAvailable(Node node);

    /**
     * Returns the number of milliseconds since the node was last checked for
     * availability. Because of its lack of precision, this should really only
     * be used for status/reporting.
     * 
     * @param node Node to check
     * 
     * @return Number of milliseconds since the node was last checked for
     *         availability
     */

    public long getLastChecked(Node node);

    /**
     * Allows external callers to provide input to the FailureDetector that an
     * error occurred when trying to access the node. The implementation is free
     * to use or ignore this input. It can be considered a "hint" to the
     * FailureDetector rather than an absolute truth. For example, it is
     * possible to call recordException for a given node and have an immediate
     * call to isAvailable return true, depending on the implementation.
     * 
     * @param node Node to check
     * @param e Exception that occurred when trying to access the node
     */

    public void recordException(Node node, Exception e);

    /**
     * Allows external callers to provide input to the FailureDetector that an
     * access to the node succeeded. As with recordException, the implementation
     * is free to use or ignore this input. It can be considered a "hint" to the
     * FailureDetector rather than gospel truth.
     * 
     * <p/>
     * 
     * <b>Note for implementors</b>: because of threading issues it's possible
     * for multiple threads to attempt access to a node and some fail and some
     * succeed. In a classic last-one-in-wins scenario, it's possible for the
     * failures to be recorded first and then the successes. It would be prudent
     * for implementations not to immediately assume that the node is then
     * available.
     * 
     * @param node Node to check
     */

    public void recordSuccess(Node node);

    public void addFailureDetectorListener(FailureDetectorListener failureDetectorListener);

    public void removeFailureDetectorListener(FailureDetectorListener failureDetectorListener);

    public void destroy();

}
