/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.routing;

import java.util.Collection;

import voldemort.cluster.Node;

/**
 * A class that denotes a route to all strategy with local preference. This
 * class is meant to be consistent with the routing hierarchy convention. It
 * simply returns the list of all nodes (just like RouteToAllStrategy) but is
 * used to indicate that extra processing will be done down the pipeline.
 * 
 * @author csoman
 * 
 */
public class RouteToAllLocalPrefStrategy extends RouteToAllStrategy {

    public RouteToAllLocalPrefStrategy(Collection<Node> nodes) {
        super(nodes);
    }

    @Override
    public String getType() {
        return RoutingStrategyType.TO_ALL_LOCAL_PREF_STRATEGY;
    }
}
