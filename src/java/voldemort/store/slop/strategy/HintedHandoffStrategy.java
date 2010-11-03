/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.store.slop.strategy;

import java.util.List;

import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;

/**
 * The equivalent of {@link RoutingStrategy} for hints.
 * <p>
 * Different strategies to decide which nodes are eligible to receive hints
 */
public interface HintedHandoffStrategy {

    /**
     * 
     * Get an ordered "preference list" of nodes eligible to receive hints for a
     * given node in case of that node's failure
     * 
     * @param origin The original node which failed to receive the request
     * @return The list of nodes eligible to receive hints for original node
     * 
     */
    public List<Node> routeHint(Node origin);
}
