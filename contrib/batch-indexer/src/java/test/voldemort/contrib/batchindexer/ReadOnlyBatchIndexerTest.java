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

package test.voldemort.contrib.batchindexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;

import voldemort.contrib.batchindexer.ReadOnlyBatchIndexMapper;
import voldemort.contrib.batchindexer.ReadOnlyBatchIndexer;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.Store;
import voldemort.store.readonly.RandomAccessFileStore;
import voldemort.store.serialized.SerializingStore;
import voldemort.versioning.Versioned;

/**
 * Unit test to check Read-Only Batch Indexer <strong>in Local mode numReduce
 * will be only one hence we will see only one node files irrespective of
 * cluster details.</strong>
 * 
 * @author bbansal
 * 
 */
public class ReadOnlyBatchIndexerTest extends TestCase {

    @SuppressWarnings("unchecked")
    public void testCSVFileBatchIndexer() throws Exception {

        // rename Files
        File dataDir = new File("contrib/batch-indexer/temp-output/text");
        if(dataDir.exists()) {
            FileDeleteStrategy.FORCE.delete(dataDir);
        }

        ToolRunner.run(new Configuration(), new TextBatchIndexer(), null);

        // rename Files
        new File(dataDir, "0.index").renameTo(new File(dataDir, "users.index"));
        new File(dataDir, "0.data").renameTo(new File(dataDir, "users.data"));

        // open Store
        SerializerDefinition serDef = new SerializerDefinition("string", "UTF-8");
        Serializer<Object> Keyserializer = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(serDef);
        Serializer<Object> Valueserializer = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(new SerializerDefinition("java-serialization"));

        Store<Object, Object> store = new SerializingStore<Object, Object>(new RandomAccessFileStore("users",
                                                                                                     dataDir,
                                                                                                     1,
                                                                                                     3,
                                                                                                     1000,
                                                                                                     100 * 1000 * 1000),
                                                                           Keyserializer,
                                                                           Valueserializer);

        // query all keys and check for value
        BufferedReader reader = new BufferedReader(new FileReader(new File("contrib/common/test-data/usersCSV.txt")));
        String line;
        while(null != (line = reader.readLine())) {

            // correct Query
            String[] tokens = line.split("\\|");
            List<Versioned<Object>> found = store.get(tokens[0]);
            String result = (String) found.get(0).getValue();
            assertEquals("Value for key should match for set value", tokens[1], result);

            // wrong query
            int changeIndex = (int) (Math.random() * tokens[0].length());
            found = store.get(tokens[0].replace(tokens[0].charAt(changeIndex), '|'));
            // found size should be 0 or not match the original value.
            if(found.size() > 0) {
                result = (String) found.get(0).getValue();
                assertNotSame("Value for key should not match for set value", tokens[1], result);
            }
        }

    }
}

class TextBatchMapper extends ReadOnlyBatchIndexMapper<LongWritable, Text> {

    @Override
    public Object getKeyBytes(LongWritable key, Text value) {
        String[] tokens = value.toString().split("\\|");
        return tokens[0];
    }

    @Override
    public Object getValueBytes(LongWritable key, Text value) {
        String[] tokens = value.toString().split("\\|");
        return tokens[1];
    }

}

class TextBatchIndexer extends ReadOnlyBatchIndexer {

    @Override
    public void configure(JobConf conf) {

        conf.set("job.name", "testCSVBatchIndexer");
        conf.set("voldemort.cluster.local.filePath", "contrib/common/config/nine-node-cluster.xml");
        conf.set("voldemort.store.local.filePath", "contrib/common/config/stores.xml");
        conf.set("voldemort.store.name", "users");

        // set inset/outset path
        FileInputFormat.addInputPaths(conf, "contrib/common/test-data/usersCSV.txt");
        FileOutputFormat.setOutputPath(conf, new Path("contrib/batch-indexer/temp-output/text"));

        conf.setMapperClass(TextBatchMapper.class);
        conf.setInputFormat(TextInputFormat.class);
    }
}
