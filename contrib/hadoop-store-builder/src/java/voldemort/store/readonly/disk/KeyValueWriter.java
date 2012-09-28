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

package voldemort.store.readonly.disk;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

// Interface used by reducers to layout the datqa on disk
public interface KeyValueWriter<K, V> {

    public static enum CollisionCounter {

        NUM_COLLISIONS,
        MAX_COLLISIONS;
    }

    public void conf(JobConf job);

    public void write(K key, Iterator<V> iterator, Reporter reporter) throws IOException;

    public void close() throws IOException;

}
