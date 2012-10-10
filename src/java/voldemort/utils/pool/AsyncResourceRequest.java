/*
 * Copyright 2012 LinkedIn, Inc
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
package voldemort.utils.pool;

/**
 * Interface for asynchronous requests for resources. Exactly one of
 * useResource, handleTimeout, or handleException expected to be invoked before,
 * or soon after, deadline specified by getDeadlineNs. Ideally, useResource is
 * only invoked before the deadline. Ideally, handleTimeout is invoked soon
 * after the deadline. If owners of an object with this interface need to take
 * action after some specified timeout, then the owner needs to set their own
 * timer.
 */
public interface AsyncResourceRequest<V> {

    /**
     * To be invoked with resource to use before deadline.
     * 
     * @param resource. resource should not be null.
     */
    void useResource(V resource);

    /**
     * Invoked sometime (soon) after deadline.
     */
    void handleTimeout();

    /**
     * Invoked upon exception trying to process resource request. Invoked before
     * or (soon) after the deadline.
     * 
     * @param e
     */
    void handleException(Exception e);

    /**
     * 
     * @return Deadline (in nanoseconds), after which handleTimeout() should be
     *         invoked.
     */
    long getDeadlineNs();

}
