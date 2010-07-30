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

package voldemort.hadoop.pig;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import voldemort.hadoop.VoldemortHadoopConfig;
import voldemort.hadoop.VoldemortInputFormat;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

@SuppressWarnings("unchecked")
public class VoldemortStore extends LoadFunc {

    private Configuration conf;

    private RecordReader reader;

    @Override
    public InputFormat getInputFormat() throws IOException {
        VoldemortInputFormat inputFormat = new VoldemortInputFormat();
        return inputFormat;
    }

    @Override
    public Tuple getNext() throws IOException {
        ByteArray key = null;
        Versioned<byte[]> value = null;

        try {

            if(!reader.nextKeyValue())
                return null;
            key = (ByteArray) reader.getCurrentKey();
            value = (Versioned<byte[]>) reader.getCurrentValue();

        } catch(InterruptedException e) {
            throw new IOException("Error reading in key/value");
        }

        if(key == null || value == null) {
            return null;
        }

        Tuple tuple = TupleFactory.getInstance().newTuple(2);
        tuple.set(0, new DataByteArray(key.get()));
        tuple.set(1, new String(value.getValue()));
        return tuple;
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.reader = reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        if(!location.startsWith("tcp://"))
            throw new IOException("The correct format is tcp://<url:port>/storeName");
        String[] subParts = location.split("/+");
        conf = job.getConfiguration();
        VoldemortHadoopConfig.setVoldemortURL(conf, subParts[0] + "//" + subParts[1]);
        VoldemortHadoopConfig.setVoldemortStoreName(conf, subParts[2]);
    }
}
