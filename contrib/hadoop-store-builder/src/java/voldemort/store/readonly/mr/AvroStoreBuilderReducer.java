package voldemort.store.readonly.mr;

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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import voldemort.store.readonly.disk.HadoopStoreWriter;
import voldemort.store.readonly.disk.KeyValueWriter;
import azkaban.common.utils.Utils;

/**
 * Take key md5s and value bytes and build a Avro read-only store from these
 * values
 */
public class AvroStoreBuilderReducer implements
        Reducer<AvroKey<ByteBuffer>, AvroValue<ByteBuffer>, Text, Text>, JobConfigurable, Closeable {

    // The Class implementing the keyvaluewriter
    // this provides a pluggable mechanism for generating your own on disk
    // format for the data and index files
    String keyValueWriterClass;
    @SuppressWarnings("rawtypes")
    KeyValueWriter writer;

    @SuppressWarnings("unchecked")
    @Override
    public void reduce(AvroKey<ByteBuffer> keyAvro,
                       Iterator<AvroValue<ByteBuffer>> iterator,
                       OutputCollector<Text, Text> collector,
                       Reporter reporter) throws IOException {

        ByteBuffer keyBuffer = keyAvro.datum();
        keyBuffer.rewind();

        byte[] keyBytes = null, valueBytes;

        keyBytes = new byte[keyBuffer.remaining()];
        keyBuffer.get(keyBytes);

        BytesWritable key = new BytesWritable(keyBytes);

        ArrayList<BytesWritable> valueList = new ArrayList();

        while(iterator.hasNext()) {
            ByteBuffer writable = iterator.next().datum();
            writable.rewind();
            // BytesWritable writable = iterator.next();
            valueBytes = null;
            valueBytes = new byte[writable.remaining()];
            writable.get(valueBytes);

            BytesWritable value = new BytesWritable(valueBytes);
            valueList.add(value);

        }

        writer.write(key, valueList.iterator(), reporter);

    }

    @Override
    public void configure(JobConf job) {

        JobConf conf = job;
        try {

            keyValueWriterClass = conf.get("writer.class");
            if(keyValueWriterClass != null)
                writer = (KeyValueWriter) Utils.callConstructor(keyValueWriterClass);
            else
                writer = new HadoopStoreWriter();

            writer.conf(job);

        } catch(Exception e) {
            // throw new RuntimeException("Failed to open Input/OutputStream",
            // e);
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {

        writer.close();
    }
}
