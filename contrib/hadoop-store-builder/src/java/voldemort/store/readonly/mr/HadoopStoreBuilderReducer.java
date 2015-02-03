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

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import voldemort.store.readonly.disk.HadoopStoreWriter;
import voldemort.store.readonly.disk.KeyValueWriter;
import voldemort.utils.ReflectUtils;

/**
 * Take key md5s and value bytes and build a read-only store from these values
 */
@SuppressWarnings("deprecation")
public class HadoopStoreBuilderReducer implements Reducer<BytesWritable, BytesWritable, Text, Text> {

    Class keyValueWriterClass = null;
    @SuppressWarnings("rawtypes")
    KeyValueWriter writer;

    /**
     * Reduce should get sorted MD5 of Voldemort key ( either 16 bytes if saving
     * keys is disabled, else 8 bytes ) as key and for value (a) node-id,
     * partition-id, value - if saving keys is disabled (b) node-id,
     * partition-id, replica-type, [key-size, value-size, key, value]* if saving
     * keys is enabled
     */
    @SuppressWarnings("unchecked")
    public void reduce(BytesWritable key,
                       Iterator<BytesWritable> iterator,
                       OutputCollector<Text, Text> output,
                       Reporter reporter) throws IOException {

        writer.write(key, iterator, reporter);

    }

    @Override
    public void configure(JobConf job) {

        try {

            keyValueWriterClass = job.getClass("writer.class", null);
            if(keyValueWriterClass != null)
                writer = (KeyValueWriter) ReflectUtils.callConstructor(keyValueWriterClass);
            else
                writer = new HadoopStoreWriter();

            writer.conf(job);

        } catch(Exception e) {
            throw new RuntimeException("Failed to open Input/OutputStream", e);
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
