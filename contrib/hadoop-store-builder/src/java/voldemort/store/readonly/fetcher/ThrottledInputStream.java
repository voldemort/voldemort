package voldemort.store.readonly.fetcher;

import voldemort.utils.EventThrottler;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link java.io.FilterInputStream} which throttles reads.
 */
public class ThrottledInputStream extends FilterInputStream {
    private final EventThrottler throttler;
    private final HdfsCopyStats stats;
    /**
     * Creates a <code>ThrottledInputStream</code>
     *
     * @param in the underlying input stream, or <code>null</code> if
     *           this instance is to be created without an underlying stream.
     * @param throttler the {@link EventThrottler} to use for throttling.
     */
    protected ThrottledInputStream(InputStream in,
                                   EventThrottler throttler,
                                   HdfsCopyStats stats) {
        super(in);
        this.throttler = throttler;
        this.stats = stats;
    }

    @Override
    public int read() throws IOException {
        int read = in.read();
        recordAndThrottle(read, 1);
        return read;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        int read = in.read(b, off, len);
        recordAndThrottle(read, read);
        return read;
    }

    private void recordAndThrottle(int readReturnValue, int bytesRead) {
        if (readReturnValue >= 0) {
            stats.recordBytesTransferred(bytesRead);
            if (throttler != null) {
                throttler.maybeThrottle(bytesRead);
            }
        }
    }
}