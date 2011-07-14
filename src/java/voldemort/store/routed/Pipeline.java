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

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.routed.action.Action;

/**
 * A Pipeline is the main conduit through which an {@link Action} is run. An
 * {@link Action} is executed in response to the Pipeline receiving an event.
 * The majority of the events are self-initiated from within the Pipeline
 * itself. The only case thus-far where external entities create events are in
 * response to asynchronous responses from servers. A {@link Response} instance
 * is created on completion of an asynchronous request and is fed back into the
 * Pipeline where an appropriate 'response handler' action is executed.
 * 
 * <p/>
 * 
 * A Pipeline instance is created per-request inside {@link RoutedStore}. This
 * is due to the fact that it includes internal state, specific to each
 * operation request (get, getAll, getVersions, put, and delete) invocation.
 */

public class Pipeline {

    public enum Event {

        STARTED,
        CONFIGURED,
        COMPLETED,
        INSUFFICIENT_SUCCESSES,
        INSUFFICIENT_ZONES,
        RESPONSES_RECEIVED,
        ERROR,
        MASTER_DETERMINED,
        ABORTED,
        HANDOFF_FINISHED;

    }

    public enum Operation {

        GET,
        GET_ALL,
        GET_VERSIONS,
        PUT,
        DELETE;

        public String getSimpleName() {
            return toString().toLowerCase().replace("_", " ");
        }

    }

    private final Operation operation;

    private final long timeout;

    private final TimeUnit unit;

    private final BlockingQueue<Event> eventQueue;

    private final Map<Event, Action> eventActions;

    private final Logger logger = Logger.getLogger(getClass());

    private volatile boolean enableHintedHandoff = false;

    private volatile boolean finished = false;

    /**
     * 
     * @param operation
     * @param timeout Timeout
     * @param unit Unit of timeout
     */

    public Pipeline(Operation operation, long timeout, TimeUnit unit) {
        this.operation = operation;
        this.timeout = timeout;
        this.unit = unit;
        this.eventQueue = new LinkedBlockingQueue<Event>();
        this.eventActions = new ConcurrentHashMap<Event, Action>();
    }

    public Operation getOperation() {
        return operation;
    }

    /**
     * Assigns the event to be handled by the given action. When a given event
     * is received, it is expected that there is an action to handle it.
     * 
     * @param event Event
     * @param action Action to invoke upon receipt of that event
     */

    public void addEventAction(Event event, Action action) {
        eventActions.put(event, action);
    }

    /**
     * Pipeline can't proceed further. If hinted handoff is enabled go to go to
     * a state where it can be performed, otherwise go straight to error state.
     */

    public void abort() {
        if(isHintedHandoffEnabled())
            addEvent(Event.ABORTED);
        else
            addEvent(Event.ERROR);
    }

    /**
     * Add an event to the queue. It will be processed in the order received.
     * 
     * @param event Event
     */

    public void addEvent(Event event) {
        if(event == null)
            throw new IllegalStateException("event must be non-null");

        if(logger.isTraceEnabled())
            logger.trace("Adding event " + event);

        eventQueue.add(event);
    }

    public boolean isHintedHandoffEnabled() {
        return enableHintedHandoff;
    }

    public void setEnableHintedHandoff(boolean enableHintedHandoff) {
        this.enableHintedHandoff = enableHintedHandoff;
    }

    public boolean isFinished() {
        return finished;
    }

    /**
     * Process events in the order as they were received.
     * 
     * <p/>
     * 
     * The overall time to process the events must be within the bounds of the
     * timeout or an {@link InsufficientOperationalNodesException} will be
     * thrown.
     */

    public void execute() {
        try {
            while(true) {
                Event event = null;

                try {
                    event = eventQueue.poll(timeout, unit);
                } catch(InterruptedException e) {
                    throw new InsufficientOperationalNodesException(operation.getSimpleName()
                                                                    + " operation interrupted!", e);
                }

                if(event == null)
                    throw new VoldemortException(operation.getSimpleName()
                                                 + " returned a null event");

                if(event.equals(Event.ERROR)) {
                    if(logger.isTraceEnabled())
                        logger.trace(operation.getSimpleName()
                                     + " request, events complete due to error");

                    break;
                } else if(event.equals(Event.COMPLETED)) {
                    if(logger.isTraceEnabled())
                        logger.trace(operation.getSimpleName() + " request, events complete");

                    break;
                }

                Action action = eventActions.get(event);

                if(action == null)
                    throw new IllegalStateException("action was null for event " + event);

                if(logger.isTraceEnabled())
                    logger.trace(operation.getSimpleName() + " request, action "
                                 + action.getClass().getSimpleName() + " to handle " + event
                                 + " event");

                action.execute(this);
            }
        } finally {
            finished = true;
        }
    }

}
