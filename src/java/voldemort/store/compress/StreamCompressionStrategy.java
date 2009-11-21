package voldemort.store.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;

/**
 * Useful base class for stream-based compression strategies.
 */
public abstract class StreamCompressionStrategy implements CompressionStrategy {

    public byte[] deflate(byte[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        OutputStream gos = wrapOutputStream(bos);
        gos.write(data);
        gos.close();
        return bos.toByteArray();
    }

    protected abstract OutputStream wrapOutputStream(OutputStream underlying) throws IOException;

    protected abstract InputStream wrapInputStream(InputStream underlying) throws IOException;

    public byte[] inflate(byte[] data) throws IOException {
        InputStream is = wrapInputStream(new ByteArrayInputStream(data));
        byte[] inflated = IOUtils.toByteArray(is);
        is.close();
        return inflated;
    }
}
