package voldemort.store.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;

/**
 * Implementation of CompressionStrategy for the gzip format.
 */
/*
 * In the future we may want to support different compression levels.
 */
public class GzipCompressionStrategy implements CompressionStrategy {

    public byte[] deflate(byte[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GZIPOutputStream gos = new GZIPOutputStream(bos);
        gos.write(data);
        gos.close();
        return bos.toByteArray();
    }

    public byte[] inflate(byte[] data) throws IOException {
        GZIPInputStream is = new GZIPInputStream(new ByteArrayInputStream(data));
        byte[] inflated = IOUtils.toByteArray(is);
        is.close();
        return inflated;
    }

    public String getType() {
        return "gzip";
    }
}
