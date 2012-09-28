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

package voldemort.store.readonly.mr.serialization;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.OutputCollector;

import voldemort.serialization.Serializer;
import azkaban.common.utils.Utils;

/**
 * Output collector that handles JSON serialization
 * 
 * @author jkreps
 * 
 */
public class JsonOutputCollector<K, V> implements OutputCollector<K, V> {

    private final Serializer<Object> keySerializer;
    private final Serializer<Object> valueSerializer;
    private final OutputCollector<BytesWritable, BytesWritable> innerCollector;

    public JsonOutputCollector(OutputCollector<BytesWritable, BytesWritable> innerCollector,
                               Serializer<Object> keySerializer,
                               Serializer<Object> valueSerializer) {
        this.innerCollector = Utils.nonNull(innerCollector);
        this.keySerializer = Utils.nonNull(keySerializer);
        this.valueSerializer = Utils.nonNull(valueSerializer);
    }

    public void collect(K key, V value) throws IOException {
        innerCollector.collect(new BytesWritable(keySerializer.toBytes(key)),
                               new BytesWritable(valueSerializer.toBytes(value)));
    }

    public Serializer<Object> getKeySerializer() {
        return keySerializer;
    }

    public Serializer<Object> getValueSerializer() {
        return valueSerializer;
    }

}
