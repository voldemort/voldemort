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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import voldemort.store.routed.ListStateData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.RequestCompletedCallback;

public class UpdateResults extends AbstractAction<ListStateData> {

    public void execute(Pipeline pipeline, Object eventData) {
        List<Object> results = new ArrayList<Object>();

        for(RequestCompletedCallback callback: pipelineData.getInterimResults()) {
            if(callback.getResult() instanceof Collection<?>)
                results.addAll((Collection<?>) callback.getResult());
            else
                results.add(callback.getResult());
        }

        pipelineData.setResults(results);

        pipeline.addEvent(completeEvent);
    }

}
