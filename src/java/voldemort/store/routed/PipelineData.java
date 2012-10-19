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

package voldemort.store.routed;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.store.routed.Pipeline.Event;

/**
 * PipelineData includes a common set of data that is used to represent the
 * state within the {@link Pipeline} as it moves from action to action. There's
 * a one-to-one correspondence between a {@link Pipeline} and
 * {@link PipelineData}, though the latter is not included as an instance
 * variable. Action implementations usually include the {@link PipelineData} as
 * an instance variable upon creation.
 * 
 * It's basically a POJO that includes some relevant state for the pipeline
 * execution:
 * 
 * There are some subclasses of {@link PipelineData} that are used to handle the
 * different types of operations:
 * 
 * <ol>
 * <li>{@link BasicPipelineData} - used by most operations; includes a list of
 * {@link Node} instances relevant to the key in the operation among other
 * values
 * <li>{@link GetAllPipelineData} - used by the "get all" operation
 * specifically, due to the fact that it includes Map data structures which
 * don't fit in well in the generic structure
 * <li>{@link PutPipelineData} - used only by the "put" operation as it includes
 * data specific to that operation
 * </ol>
 * 
 * @param <K> Type for the key used in the request
 * @param <V> Type for the value returned by the call
 * 
 * @see Pipeline
 * @see PipelineData
 * @see BasicPipelineData
 * @see GetAllPipelineData
 * @see PutPipelineData
 */

public abstract class PipelineData<K, V> {

    private final List<Response<K, V>> responses;

    protected final List<Exception> failures;

    protected VoldemortException fatalError;

    protected volatile String storeName;

    protected final List<Node> failedNodes;

    protected List<Node> replicationSet;

    protected PipelineRoutedStats stats;

    public void setStats(PipelineRoutedStats stats) {
        this.stats = stats;
    }

    public List<Node> getReplicationSet() {
        return replicationSet;
    }

    public void setReplicationSet(List<Node> replicationSet) {
        this.replicationSet = replicationSet;
    }

    public PipelineData() {
        this.responses = new ArrayList<Response<K, V>>();
        this.failures = new ArrayList<Exception>();
        this.failedNodes = new CopyOnWriteArrayList<Node>();
    }

    /**
     * Returns is a list of responses that are received by requests to remote
     * Voldemort nodes, either synchronous or asynchronous.
     * 
     * @return List of {@link Response} instances
     */

    public List<Response<K, V>> getResponses() {
        return responses;
    }

    /**
     * Returns a "fatal" error that occurred while attempting to contact the
     * remote Voldemort nodes.
     * 
     * <p/>
     * 
     * If a hard error occurs, fatalError will be populated and the
     * {@link Event#COMPLETED} event will be pushed onto the Pipeline event
     * stack.
     * 
     * @return {@link VoldemortException}, or null if no fatal error occurred
     *         during processing
     */

    public VoldemortException getFatalError() {
        return fatalError;
    }

    public void setFatalError(VoldemortException fatalError) {
        reportException(fatalError);
        this.fatalError = fatalError;
    }

    /**
     * Returns a list of zero or more errors that occurred while attempting to
     * contact the remote Voldemort node.
     * 
     * @return List of non-fatal exceptions
     */

    public List<Exception> getFailures() {
        return failures;
    }

    /**
     * Adds an error to the list errors that occurred while attempting to
     * contact the remote Voldemort node.
     * 
     * @param e Exception
     */

    public void recordFailure(Exception e) {
        reportException(e);
        this.failures.add(e);
    }

    public void addFailedNode(Node node) {
        failedNodes.add(node);
    }

    public List<Node> getFailedNodes() {
        return failedNodes;
    }

    public String getStoreName() {
        return storeName;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public void reportException(Exception e) {
        if(stats != null) {
            stats.reportException(e);
        }
    }
}
