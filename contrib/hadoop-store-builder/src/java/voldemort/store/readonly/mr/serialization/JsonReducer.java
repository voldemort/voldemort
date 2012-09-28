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
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import voldemort.serialization.Serializer;

/**
 * Base reducer class that uses JSON serialization
 * 
 * @author jkreps
 * 
 */
public abstract class JsonReducer extends JsonConfigurable implements
        Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

    public void configure(JobConf conf) {
        setInputKeySerializer(getSchemaFromJob(conf, "mapper.output.key.schema"));
        setInputValueSerializer(getSchemaFromJob(conf, "mapper.output.value.schema"));
        setOutputKeySerializer(getSchemaFromJob(conf, "reducer.output.key.schema"));
        setOutputValueSerializer(getSchemaFromJob(conf, "reducer.output.value.schema"));

        // set comparator for input Key Schema
        if(conf.getBoolean("use.json.comparator", false)) {
            conf.setOutputKeyComparatorClass(JsonDeserializerComparator.class);
            conf.set("json.schema", conf.get("mapper.output.key.schema"));
        }
        setConfigured(true);
    }

    public abstract void reduceObjects(Object key,
                                       Iterator<Object> values,
                                       OutputCollector<Object, Object> collector,
                                       Reporter reporter) throws IOException;

    public void reduce(BytesWritable key,
                       Iterator<BytesWritable> values,
                       OutputCollector<BytesWritable, BytesWritable> output,
                       Reporter reporter) throws IOException {
        reduceObjects(getInputKeySerializer().toObject(key.get()),
                      new TranslatingIterator(getInputValueSerializer(), values),
                      getOutputCollector(output),
                      reporter);
    }

    protected OutputCollector<Object, Object> getOutputCollector(OutputCollector<BytesWritable, BytesWritable> output) {
        return new JsonOutputCollector<Object, Object>(output,
                                                       getOutputKeySerializer(),
                                                       getOutputValueSerializer());
    }

    private static class TranslatingIterator implements Iterator<Object> {

        private final Serializer serializer;
        private final Iterator<BytesWritable> inner;

        public TranslatingIterator(Serializer serializer, Iterator<BytesWritable> inner) {
            this.serializer = serializer;
            this.inner = inner;
        }

        public boolean hasNext() {
            return inner.hasNext();
        }

        public Object next() {
            return serializer.toObject(inner.next().get());
        }

        public void remove() {
            inner.remove();
        }
    }

}
