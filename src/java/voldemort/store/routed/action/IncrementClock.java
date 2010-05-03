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
import voldemort.store.routed.PutPipelineData;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class IncrementClock extends AbstractAction<ByteArray, Void, PutPipelineData> {

    private final Versioned<byte[]> versioned;

    private final Time time;

    public IncrementClock(PutPipelineData pipelineData,
                          Event completeEvent,
                          Versioned<byte[]> versioned,
                          Time time) {
        super(pipelineData, completeEvent);
        this.versioned = versioned;
        this.time = time;
    }

    public void execute(Pipeline pipeline) {
        if(logger.isTraceEnabled())
            logger.trace(pipeline.getOperation().getSimpleName() + " versioning data - was: "
                         + versioned.getVersion());

        // Okay looks like it worked, increment the version for the caller
        VectorClock versionedClock = (VectorClock) versioned.getVersion();
        versionedClock.incrementVersion(pipelineData.getMaster().getId(), time.getMilliseconds());

        if(logger.isTraceEnabled())
            logger.trace(pipeline.getOperation().getSimpleName() + " versioned data - now: "
                         + versioned.getVersion());

        pipeline.addEvent(completeEvent);
    }

}
