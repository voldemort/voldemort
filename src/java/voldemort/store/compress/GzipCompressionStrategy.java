package voldemort.store.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Implementation of CompressionStrategy for the gzip format.
 */
public class GzipCompressionStrategy extends StreamCompressionStrategy {

    @Override
    protected OutputStream wrapOutputStream(OutputStream underlying) throws IOException {
        return new GZIPOutputStream(underlying);
    }

    @Override
    protected InputStream wrapInputStream(InputStream underlying) throws IOException {
        return new GZIPInputStream(underlying);
    }

    public String getType() {
        return "gzip";
    }
}
