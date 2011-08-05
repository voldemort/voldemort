package voldemort.server.rebalance.async;

import java.io.IOException;
import java.util.concurrent.SynchronousQueue;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.server.protocol.admin.AsyncOperation;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

public class DonorBasedRebalancePusherSlave extends AsyncOperation {

    protected final static Logger logger = Logger.getLogger(DonorBasedRebalancePusherSlave.class);

    private final static Pair<ByteArray, Versioned<byte[]>> END = Pair.create(null, null);

    private int nodeId;
    private SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>> queue;
    private String storeName;
    private AdminClient adminClient;

    public DonorBasedRebalancePusherSlave(int id,
                                          String description,
                                          int nodeId,
                                          SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>> queue,
                                          String storeName,
                                          AdminClient adminClient) {
        super(id, description);
        this.nodeId = nodeId;
        this.queue = queue;
        this.storeName = storeName;
        this.adminClient = adminClient;
    }

    @Override
    public void operate() throws Exception {
        boolean operationCompleted = false;
        while(!operationCompleted && !getStatus().isComplete()) {
            try {
                ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> nodeIterator = new ClosableIterator<Pair<ByteArray, Versioned<byte[]>>>() {

                    public void close() {}

                    public boolean hasNext() {
                        // Empty => Not yet finished
                        Pair<ByteArray, Versioned<byte[]>> element = queue.poll();
                        if(element != null && element.equals(END)) {
                            return false;
                        } else {
                            return true;
                        }
                    }

                    public Pair<ByteArray, Versioned<byte[]>> next() {
                        try {
                            return queue.take();
                        } catch(InterruptedException e) {
                            logger.info("Next did not return anything");
                        }
                        return null;
                    }

                    public void remove() {
                        throw new VoldemortException("Remove not supported");
                    }

                };
                adminClient.updateEntries(nodeId, storeName, nodeIterator, null);
                // once we get out of updateEntries, we finished all keys
                operationCompleted = true;
            } catch(VoldemortException e) {
                if(e.getCause() instanceof IOException) {
                    // we terminated due to remote error, keep retrying after
                    // sleeping for a bit
                    Thread.sleep(10000);
                } else {
                    throw e;
                }
            }
        }
    }

    @Override
    public void stop() {
        setCompletion();
    }

    public synchronized void setCompletion() {
        if(!getStatus().isComplete()) {
            try {
                queue.put(END);
            } catch(InterruptedException e) {
                logger.info("Unable to send termination message to pusher slave due to the following reason: "
                            + e.getMessage());
            } finally {
                markComplete();
                notifyAll();
            }
        }
    }

    public synchronized void waitCompletion() {
        while(!getStatus().isComplete()) {
            try {
                logger.info("Waiting for the completion, with 10s timeout, of pusher slave for "
                            + getStatus().getDescription() + "with id=" + getStatus().getId());
                // check for status every 10 seconds
                wait(10000);
            } catch(InterruptedException e) {

            }
        }
    }

}
