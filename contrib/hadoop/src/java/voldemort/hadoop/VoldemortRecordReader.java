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

package voldemort.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

public class VoldemortRecordReader extends RecordReader<ByteArray, Versioned<byte[]>> {

    private AdminClient adminClient;
    private Iterator<Pair<ByteArray, Versioned<byte[]>>> iter = null;
    private Pair<ByteArray, Versioned<byte[]>> currentPair = null;

    @Override
    public void close() throws IOException {
        adminClient.stop();
    }

    @Override
    public ByteArray getCurrentKey() throws IOException, InterruptedException {
        return currentPair.getFirst();
    }

    @Override
    public Versioned<byte[]> getCurrentValue() throws IOException, InterruptedException {
        return currentPair.getSecond();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        VoldemortInputSplit voldemortSplit = (VoldemortInputSplit) split;
        this.adminClient = new AdminClient("tcp://" + voldemortSplit.getHostName() + ":"
                                           + voldemortSplit.getAdminPort(), new AdminClientConfig());
        List<Integer> partitionIds = new ArrayList<Integer>();
        partitionIds.addAll(adminClient.getAdminClientCluster()
                                       .getNodeById(voldemortSplit.getNodeId())
                                       .getPartitionIds());
        this.iter = adminClient.fetchEntries(voldemortSplit.getNodeId(),
                                             voldemortSplit.getStoreName(),
                                             partitionIds,
                                             null,
                                             true);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!iter.hasNext())
            return false;
        currentPair = iter.next();
        return true;
    }

}
