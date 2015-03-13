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

package voldemort.store.readonly.mr.azkaban;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import org.apache.log4j.Logger;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.readonly.mr.utils.HadoopUtils;
import voldemort.utils.Props;

/**
 * A test job that throws an exception
 * 
 * @author bbansal Required Properties
 *         <ul>
 *         <li>voldemort.cluster.file</li>
 *         <li>voldemort.store.name</li>
 *         <li>input.path</li>
 *         <li>dest.path</li>
 *         <li>source.host</li>
 *         <li>dest.host</li>
 *         </ul>
 */
public abstract class AbstractVoldemortBatchCopyJob extends AbstractHadoopJob {

    public AbstractVoldemortBatchCopyJob(String name, Props props) throws IOException {
        super(name, props, Logger.getLogger(AbstractVoldemortBatchCopyJob.class.getName()));
    }

    public void run() throws Exception {
        JobConf conf = new JobConf();
        HadoopUtils.copyInAllProps(props, conf);

        Cluster cluster = HadoopUtils.readCluster(props.get("voldemort.cluster.file"), conf);
        final String storeName = props.get("voldemort.store.name");
        final Path inputDir = new Path(props.get("input.path"));

        ExecutorService executors = Executors.newFixedThreadPool(cluster.getNumberOfNodes());
        final Semaphore semaphore = new Semaphore(0, false);
        final AtomicInteger countSuccess = new AtomicInteger(0);
        final Map<Integer, Boolean> succeeded = new HashMap<Integer, Boolean>();
        final String destinationDir = props.get("dest.path");
        final String sourceHost = props.getString("src.host", "localhost");

        for(final Node node: cluster.getNodes()) {

            executors.execute(new Runnable() {

                public void run() {
                    int id = node.getId();
                    String indexFile = inputDir + "/" + storeName + ".index" + "_"
                                       + Integer.toString(id);
                    String dataFile = inputDir + "/" + storeName + ".data" + "_"
                                      + Integer.toString(id);

                    String host = node.getHost();
                    try {
                        // copyFileToLocal(sourceHost,
                        // indexFile,
                        // host,
                        // VoldemortSwapperUtils.getIndexDestinationFile(node.getId(),
                        // destinationDir));
                        // copyFileToLocal(sourceHost,
                        // dataFile,
                        // host,
                        // VoldemortSwapperUtils.getDataDestinationFile(node.getId(),
                        // destinationDir));

                        succeeded.put(node.getId(), true);
                        countSuccess.incrementAndGet();
                    } catch(Exception e) {
                        error("copy to Remote node failed for node:" + node.getId(), e);
                    }

                    semaphore.release();
                }
            });
        }

        // wait for all operations to complete
        semaphore.acquire(cluster.getNumberOfNodes());

        try {
            if(countSuccess.get() == cluster.getNumberOfNodes()
               || props.getBoolean("swap.partial.index", false)) {
                int counter = 0;
                // lets try to swap only the successful nodes
                for(Node node: cluster.getNodes()) {
                    // data refresh succeeded
                    if(succeeded.get(node.getId())) {
                        VoldemortSwapperUtils.doSwap(storeName, node, destinationDir);
                        counter++;
                    }
                }
                info(counter + " node out of " + cluster.getNumberOfNodes()
                     + " refreshed with fresh index/data for store '" + storeName + "'");
            } else {
                error("Failed to copy Index Files for the entire cluster.");
            }
        } finally {
            // stop all executors Now
            executors.shutdown();
        }
    }

}
