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

package voldemort.contrib.batchindexer.performance;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.SequenceFileRecordReader;

import voldemort.TestUtils;
import voldemort.performance.PerformanceTest;
import voldemort.server.VoldemortConfig;
import voldemort.store.Store;
import voldemort.store.mysql.MysqlStorageConfiguration;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Props;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

@SuppressWarnings("deprecation")
public class MysqlBuildPerformanceTest {

    public static void main(String[] args) throws FileNotFoundException, IOException {
        if(args.length != 3)
            Utils.croak("USAGE: java " + MysqlBuildPerformanceTest.class.getName()
                        + "serverPropsFile storeName jsonSequenceDataFile");

        String serverPropsFile = args[0];
        String storeName = args[1];
        String jsonDataFile = args[2];

        final Store<ByteArray, byte[], byte[]> store = new MysqlStorageConfiguration(new VoldemortConfig(new Props(new File(serverPropsFile)))).getStore(TestUtils.makeStoreDefinition(storeName), TestUtils.makeSingleNodeRoutingStrategy());

        final AtomicInteger obsoletes = new AtomicInteger(0);

        Path jsonFilePath = new Path(jsonDataFile);
        FileStatus jsonFileStatus = jsonFilePath.getFileSystem(new Configuration())
                                                .listStatus(jsonFilePath)[0];
        final SequenceFileRecordReader<BytesWritable, BytesWritable> reader = new SequenceFileRecordReader<BytesWritable, BytesWritable>(new Configuration(),
                                                                                                                                         new FileSplit(jsonFilePath,
                                                                                                                                                       0,
                                                                                                                                                       jsonFileStatus.getLen(),
                                                                                                                                                       (String[]) null));

        PerformanceTest readWriteTest = new PerformanceTest() {

            @Override
            public void doOperation(int index) throws Exception {
                try {

                    BytesWritable key = new BytesWritable();
                    BytesWritable value = new BytesWritable();

                    reader.next(key, value);
                    store.put(new ByteArray(ByteUtils.copy(key.get(), 0, key.getSize())),
                              Versioned.value(ByteUtils.copy(value.get(), 0, value.getSize())), null);
                } catch(ObsoleteVersionException e) {
                    obsoletes.incrementAndGet();
                }
            }
        };
        readWriteTest.run(1000, 1);
        System.out.println("MySQl write throuhput with one thread:");
        readWriteTest.printStats();
    }
}
