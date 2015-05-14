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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * A Mapper that handles JSON serialization
 * 
 * @author jkreps
 * 
 */
public abstract class JsonMapper extends JsonConfigurable implements
        Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

    public abstract void mapObjects(Object key,
                                    Object value,
                                    OutputCollector<Object, Object> output,
                                    Reporter reporter) throws IOException;

    public void configure(JobConf conf) {
        setInputKeySerializer(getSchemaFromJob(conf, "mapper.input.key.schema"));
        setInputValueSerializer(getSchemaFromJob(conf, "mapper.input.value.schema"));
        setOutputKeySerializer(getSchemaFromJob(conf, "mapper.output.key.schema"));
        setOutputValueSerializer(getSchemaFromJob(conf, "mapper.output.value.schema"));

        // set comparator for input Key Schema
        if(conf.getBoolean("use.json.comparator", false)) {
            conf.setOutputKeyComparatorClass(JsonDeserializerComparator.class);
            conf.set("json.schema", conf.get("mapper.output.key.schema"));
        }
        setConfigured(true);
    }

    @SuppressWarnings("unchecked")
    public void map(BytesWritable key,
                    BytesWritable value,
                    OutputCollector<BytesWritable, BytesWritable> output,
                    Reporter reporter) throws IOException {
        if(!isConfigured())
            throw new IllegalStateException("JsonMapper's configure method wasn't called.  Please make sure that super.configure() is called.");

        mapObjects(getInputKeySerializer().toObject(key.get()),
                   getInputValueSerializer().toObject(value.get()),
                   getOutputCollector(output),
                   reporter);
    }

    protected OutputCollector<Object, Object> getOutputCollector(OutputCollector<BytesWritable, BytesWritable> output) {
        return new JsonOutputCollector<Object, Object>(output,
                                                       getOutputKeySerializer(),
                                                       getOutputValueSerializer());
    }

}
