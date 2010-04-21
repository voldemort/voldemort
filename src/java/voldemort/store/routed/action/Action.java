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

package voldemort.store.routed.action;

import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;

/**
 * An Action is executed in response to the {@link Pipeline} receiving an
 * {@link Event}. An Action is a discrete portion of logic that forms part of
 * the overall process that executes a given operation. There’s no clear
 * standard about how much or how little logic is performed in a given Action,
 * but there are intuitive separations in the logic that form natural
 * boundaries.
 * 
 * <p/>
 * 
 * Actions are mapped to events by the {@link Pipeline} via the
 * {@link Pipeline#setEventActions(java.util.Map)} method.
 * 
 * @see Pipeline#setEventActions(java.util.Map)
 */

public interface Action {

    /**
     * Executes some portion of the overall logic in the routing pipeline.
     * 
     * @param pipeline {@link Pipeline} instance of which this action is a part
     * @param eventData Data included as part of the event, or null if not
     *        applicable
     */

    public void execute(Pipeline pipeline, Object eventData);

}
