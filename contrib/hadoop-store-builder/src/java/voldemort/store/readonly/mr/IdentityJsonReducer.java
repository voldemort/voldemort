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

package voldemort.store.readonly.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import voldemort.store.readonly.mr.serialization.JsonReducer;

public class IdentityJsonReducer extends JsonReducer {

    @Override
    public void reduceObjects(Object key,
                              Iterator<Object> values,
                              OutputCollector<Object, Object> collector,
                              Reporter reporter) throws IOException {
        while(values.hasNext()) {
            collector.collect(key, values.next());
        }
    }
}
