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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import voldemort.hadoop.VoldemortHadoopConfig;
import voldemort.hadoop.VoldemortInputFormat;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import java.io.IOException;

/**
 * Voldemort store which exposes values as DataByteArrays. Useful for loading
 * binary format data (e.g protobufs, thrift).
 *
 * To use with Twitter's Elephant-Bird:
 *
 * <pre>
 *     dataset = LOAD 'tcp://localhost:6666/storename' USING BinaryVoldemortStore();
 *     DEFINE XProtoFormat x.x.x.pig.piggybank.XProtobufBytesToTuple();
 *     result = FOREACH dataset GENERATE $0 as key, XProtoFormat($1).fieldName as fieldName;
 * </pre>
 */
public class BinaryVoldemortStore extends AbstractVoldemortStore {
    @Override
    protected Tuple extractTuple(ByteArray key, Versioned<byte[]> value) throws ExecException {
        Tuple tuple = TupleFactory.getInstance().newTuple(2);
        tuple.set(0, new DataByteArray(key.get()));
        tuple.set(1, new DataByteArray(value.getValue()));
        return tuple;

    }
}
