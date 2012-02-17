package voldemort.store.compress;

import org.iq80.snappy.SnappyInputStream;
import org.iq80.snappy.SnappyOutputStream;
import voldemort.annotations.Experimental;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Implementation of CompressionStrategy for the Snappy format. Snappy is optimized
 * for speed.
 *
 * TODO Use block encoding instead of streams for better performance, see:
 * https://github.com/dain/snappy/issues/4. Also be aware that the stream format may not be finalised
 * yet.
 */
@Experimental
public class SnappyCompressionStrategy extends StreamCompressionStrategy {

    public String getType() {
        return "snappy";
    }

    protected OutputStream wrapOutputStream(OutputStream underlying) throws IOException {
        return new SnappyOutputStream(underlying);
    }

    protected InputStream wrapInputStream(InputStream underlying) throws IOException {
        return new SnappyInputStream(underlying);
    }

}
