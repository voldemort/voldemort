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
import org.apache.log4j.Logger;

import voldemort.store.readonly.disk.HadoopStoreWriterPerBucket;
import voldemort.store.readonly.disk.KeyValueWriter;
import azkaban.common.utils.Utils;

/**
 * Take key md5s and value bytes and build a read-only store from these values
 */
@SuppressWarnings("deprecation")
public class HadoopStoreBuilderReducerPerBucket extends AbstractStoreBuilderConfigurable implements
        Reducer<BytesWritable, BytesWritable, Text, Text> {

    private static final Logger logger = Logger.getLogger(HadoopStoreBuilderReducerPerBucket.class);

    String keyValueWriterClass;
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

            keyValueWriterClass = job.get("writer.class");
            if(keyValueWriterClass != null)
                writer = (KeyValueWriter) Utils.callConstructor(keyValueWriterClass);
            else
                writer = new HadoopStoreWriterPerBucket();

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
