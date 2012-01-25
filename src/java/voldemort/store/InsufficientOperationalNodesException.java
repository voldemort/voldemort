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

package voldemort.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Node;

/**
 * Thrown if an operation fails due to too few reachable nodes.
 * 
 * 
 */
public class InsufficientOperationalNodesException extends VoldemortException {

    private static final long serialVersionUID = 1L;

    private Collection<? extends Throwable> causes;

    private static final Logger logger = Logger.getLogger(InsufficientOperationalNodesException.class);

    public InsufficientOperationalNodesException(String s, Throwable e) {
        super(s, e);
        causes = Collections.singleton(e);
    }

    public InsufficientOperationalNodesException(String s) {
        super(s);
        causes = Collections.emptyList();
    }

    public InsufficientOperationalNodesException(Throwable e) {
        super(e);
        causes = Collections.singleton(e);
    }

    public InsufficientOperationalNodesException(Collection<? extends Throwable> failures) {
        this("Insufficient operational nodes to immediately satisfy request.", failures);
    }

    public InsufficientOperationalNodesException(String message,
                                                 Collection<? extends Throwable> failures) {
        super(message, failures.size() > 0 ? failures.iterator().next() : null);
        this.causes = failures;
    }

    public InsufficientOperationalNodesException(String message,
                                                 List<Node> replicationSet,
                                                 List<Node> preferenceList,
                                                 List<Node> failedList,
                                                 Collection<? extends Throwable> failures) {
        this(message + " Original replication set :" + stripNodeIds(replicationSet)
                     + " Known failed nodes before operation :"
                     + stripNodeIds(difference(replicationSet, preferenceList))
                     + " Estimated live nodes in preference list :" + stripNodeIds(preferenceList)
                     + " New failed nodes during operation :"
                     + stripNodeIds(difference(failedList, replicationSet)),
             failures.size() > 0 ? failures.iterator().next() : null);
        if(logger.isDebugEnabled()) {
            logger.debug(this.getMessage());
        }
    }

    public Collection<? extends Throwable> getCauses() {
        return this.causes;
    }

    /**
     * Helper method to get a list of node ids.
     * 
     * @param nodeList
     */
    private static List<Integer> stripNodeIds(List<Node> nodeList) {
        List<Integer> nodeidList = new ArrayList<Integer>();
        if(nodeList != null) {
            for(Node node: nodeList) {
                nodeidList.add(node.getId());
            }
        }
        return nodeidList;
    }

    /**
     * Computes A-B
     * 
     * @param listA
     * @param listB
     * @return
     */
    private static List<Node> difference(List<Node> listA, List<Node> listB) {
        if(listA != null && listB != null)
            listA.removeAll(listB);
        return listA;
    }
}
