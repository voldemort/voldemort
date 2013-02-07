package voldemort.utils;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import voldemort.store.routed.NodeValue;
import voldemort.utils.ConsistencyFix.BadKeyResult;
import voldemort.utils.ConsistencyFix.Status;

// TODO: Move (back) into ConsistencyFix?
class ConsistencyFixRepairPutter implements Runnable {

    // TODO: Add stats shared across all putters (invocations, successes,
    // etc.)
    private final ConsistencyFix consistencyFix;
    private final String keyInHexFormat;
    private final BlockingQueue<BadKeyResult> badKeyQOut;
    private final List<NodeValue<ByteArray, byte[]>> nodeValues;
    private final boolean verbose;

    ConsistencyFixRepairPutter(ConsistencyFix consistencyFix,
                               String keyInHexFormat,
                               BlockingQueue<BadKeyResult> badKeyQOut,
                               List<NodeValue<ByteArray, byte[]>> nodeValues,
                               boolean verbose) {
        this.consistencyFix = consistencyFix;
        this.keyInHexFormat = keyInHexFormat;
        this.badKeyQOut = badKeyQOut;
        this.nodeValues = nodeValues;
        this.verbose = verbose;
    }

    private String myName() {
        return Thread.currentThread().getName();
    }

    @Override
    public void run() {
        Status consistencyFixStatus = consistencyFix.doRepairPut(nodeValues, verbose);
        if(consistencyFixStatus != Status.SUCCESS) {
            try {
                badKeyQOut.put(consistencyFix.new BadKeyResult(keyInHexFormat, consistencyFixStatus));
            } catch(InterruptedException ie) {
                System.err.println("RepairPutter thread " + myName() + " interruped.");
            }
        }
    }

}