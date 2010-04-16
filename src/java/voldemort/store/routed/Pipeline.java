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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.routed.action.Action;

public class Pipeline {

    public enum Event {

        STARTED,
        CONFIGURED,
        COMPLETED,
        INSUFFICIENT_SUCCESSES,
        RESPONSE_RECEIVED,
        RESPONSES_RECEIVED,
        NOP,
        ERROR,
        MASTER_DETERMINED;

    }

    public enum Operation {

        GET,
        GET_ALL,
        GET_VERSIONS,
        PUT,
        DELETE;

        public String getSimpleName() {
            return StringUtils.capitalize(toString().toLowerCase().replace("_", " "));
        }

    }

    private static class EventData {

        private final Event event;

        private final Object data;

        private EventData(Event event, Object data) {
            this.event = event;
            this.data = data;
        }

    }

    private final Operation operation;

    private final BlockingQueue<EventData> eventDataQueue;

    private Map<Event, Action> eventActions;

    private final Logger logger = Logger.getLogger(getClass());

    public Pipeline(Operation operation) {
        this.operation = operation;
        this.eventDataQueue = new LinkedBlockingQueue<EventData>();
    }

    public Operation getOperation() {
        return operation;
    }

    public Map<Event, Action> getEventActions() {
        return eventActions;
    }

    public void setEventActions(Map<Event, Action> eventActions) {
        this.eventActions = eventActions;
    }

    public void addEvent(Event event) {
        addEvent(event, null);
    }

    public void addEvent(Event event, Object data) {
        if(logger.isTraceEnabled())
            logger.trace("Adding event " + event);

        eventDataQueue.add(new EventData(event, data));
    }

    public void processEvents(long timeout, TimeUnit unit) {
        long start = System.nanoTime();

        while(true) {
            EventData eventData = null;

            try {
                eventData = eventDataQueue.poll(timeout, unit);
            } catch(InterruptedException e) {
                throw new InsufficientOperationalNodesException(operation.getSimpleName()
                                                                + " operation interrupted!", e);
            }

            if((System.nanoTime() - start) > unit.toNanos(timeout))
                throw new InsufficientOperationalNodesException(operation.getSimpleName()
                                                                + " operation interrupted!");

            if(eventData.event.equals(Event.ERROR)) {
                if(logger.isTraceEnabled())
                    logger.trace(operation.getSimpleName()
                                 + " request, events complete due to error");

                break;
            } else if(eventData.event.equals(Event.COMPLETED)) {
                if(logger.isTraceEnabled())
                    logger.trace(operation.getSimpleName() + " request, events complete");

                break;
            }

            if(eventData.event.equals(Event.NOP))
                continue;

            Action action = eventActions.get(eventData.event);

            if(action == null)
                throw new IllegalStateException("action was null for event " + eventData.event);

            if(logger.isTraceEnabled())
                logger.trace(operation.getSimpleName() + " request, action "
                             + action.getClass().getSimpleName() + " to handle " + eventData.event
                             + " event");

            action.execute(this, eventData.data);
        }
    }

}
