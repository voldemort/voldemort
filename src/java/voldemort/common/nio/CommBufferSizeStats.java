package voldemort.common.nio;

import org.apache.commons.lang.mutable.MutableLong;

/**
 * Statistics object to track the communication buffer sizes across all the
 * connections, handled by the selector managers
 * 
 */
public class CommBufferSizeStats {

    private MutableLong commReadBufferSizeBytes;

    private MutableLong commWriteBufferSizeBytes;

    public CommBufferSizeStats() {
        commReadBufferSizeBytes = new MutableLong(0);
        commWriteBufferSizeBytes = new MutableLong(0);
    }

    public MutableLong getCommReadBufferSizeTracker() {
        return commReadBufferSizeBytes;
    }

    public MutableLong getCommWriteBufferSizeTracker() {
        return commWriteBufferSizeBytes;
    }
}
