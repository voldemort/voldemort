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
 * used by request routing to avoid unavailable servers.
 * 
 * <p/>
 * 
 * The isAvailable method is a simple boolean operation to determine if the node
 * in question is available. As expected, the result of this call is an
 * approximation given the likely possible race conditions. However, the
 * FailureDetector should do its best to determine the then-current state of the
 * cluster to produce a minimum of false negatives and false positives.
 * 
 * <p/>
 * 
 * Implementations can differ dramatically in how they determine node
 * availability. Some implementations may rely on external users of the API to
 * invoke the recordException and recordSuccess to determine availability. The
 * result is that the FailureDetector itself performs little logic other than
 * bookkeeping; it relies on users of the API to update the status as well check
 * it. For example, if contact with a node is attempted and then failed, a
 * caller may invoke the recordException API to inform the FailureDetector that
 * an error has taken place for the node. The FailureDetector itself hasn't
 * really determined that itself.
 * 
 * <p/>
 * 
 * However, other implementations may be more selective in using results of any
 * external users' calls to the recordException and/or recordSuccess methods.
 * Implementations may use these error/success calls as "hints" or may ignore
 * them outright. For example, a heartbeat style implementation may pay little
 * to no attention when its recordException and/or recordSuccess methods are
 * called and instead rely heavily or completely on its own determination of
 * node availability.
 * 
 * @author jay
 * 
 * @see RoutedStore
 */

public interface FailureDetector {

    public boolean isAvailable(Node node);

    public long getLastChecked(Node node);

    public void recordException(Node node, Exception e);

    public void recordSuccess(Node node);

}
