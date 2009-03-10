package voldemort.contrib.batchswapper;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.contrib.utils.ContribUtils;
import voldemort.utils.Props;

/**
 * voldemort swap index job. rsync and swap index/data files for Read-Only store
 * in voldemort cluster. Reads from local filesystem and writes to remote node.
 * 
 * @author bbansal
 *         <p>
 *         Required Properties
 *         <ul>
 *         <li>voldemort.cluster.local.filePath: String</li>
 *         <li>voldemort.store.name: String</li>
 *         <li>source.local.path: String</li>
 *         <li>destination.remote.path: String</li>
 *         </ul>>
 */
public abstract class ReadOnlyBatchIndexSwapper {

    private static Logger logger = Logger.getLogger(ReadOnlyBatchIndexSwapper.class);

    public void run() throws Throwable {

        final Props props = new Props();

        // get user settings.
        configure(props);

        Cluster cluster = ContribUtils.getVoldemortClusterDetails(props.get("voldemort.cluster.local.filePath"));
        String storeName = props.get("voldemort.store.name");
        final Path inputDir = new Path(props.get("source.local.path"));

        ExecutorService executors = Executors.newFixedThreadPool(cluster.getNumberOfNodes());
        final Semaphore semaphore = new Semaphore(0, false);
        final boolean[] succeeded = new boolean[cluster.getNumberOfNodes()];
        final String destinationDir = props.get("destination.remote.path");

        for(final Node node: cluster.getNodes()) {

            executors.execute(new Runnable() {

                public void run() {
                    int id = node.getId();
                    String indexFile = inputDir + "/" + Integer.toString(id) + ".index";
                    String dataFile = inputDir + "/" + Integer.toString(id) + ".data";

                    if(!(new File(indexFile).exists())) {
                        logger.warn("IndexFile for node " + id + " not available path:" + indexFile);
                    }
                    if(!(new File(dataFile).exists())) {
                        logger.warn("DataFile for node " + id + " not available path:" + dataFile);
                    }

                    if(new File(indexFile).exists() && new File(indexFile).exists()) {
                        String host = node.getHost();

                        boolean index = false;
                        boolean data = false;

                        try {
                            index = copyRemoteFile(host,
                                                   indexFile,
                                                   SwapperUtils.getIndexDestinationFile(node.getId(),
                                                                                        destinationDir));
                            data = copyRemoteFile(host,
                                                  dataFile,
                                                  SwapperUtils.getDataDestinationFile(node.getId(),
                                                                                      destinationDir));
                        } catch(IOException e) {
                            logger.error("copy to Remote node failed for node:" + node.getId(), e);
                        }

                        if(index && data) {
                            succeeded[node.getId()] = true;
                        }
                    }
                    semaphore.release();
                }
            });
        }

        // wait for all operations to complete
        semaphore.acquire(cluster.getNumberOfNodes());

        int counter = 0;
        // lets try to swap only the successful nodes
        for(Node node: cluster.getNodes()) {
            // data refresh succeeded
            if(succeeded[node.getId()]) {
                SwapperUtils.doSwap(storeName, node, destinationDir);
                counter++;
            }
        }
        logger.info(counter + " node out of " + cluster.getNumberOfNodes()
                    + " refreshed with fresh index/data for store '" + storeName + "'");
    }

    /**
     * <strong>configure must set these properties.</strong>
     * <ul>
     * <li>voldemort.cluster.local.filePath: String</li>
     * <li>voldemort.store.name: String</li>
     * <li>source.local.path: String</li>
     * <li>destination.remote.path: String</li>
     * </ul>
     * 
     * 
     * @return
     */
    public abstract void configure(Props props);

    /**
     * copy local source file to remote destination
     * 
     * @param hostname
     * @param source
     * @param destination
     * @return
     * @throws IOException
     */
    public abstract boolean copyRemoteFile(String hostname, String source, String destination)
            throws IOException;
}
