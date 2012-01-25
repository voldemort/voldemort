package voldemort.store.compress;

import java.io.IOException;

import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;

/**
 * Implementation of CompressionStrategy for the LZF format. LZF is optimized
 * for speed.
 */
public class LzfCompressionStrategy implements CompressionStrategy {

    public String getType() {
        return "lzf";
    }

    public byte[] deflate(byte[] data) throws IOException {
        return LZFEncoder.encode(data);
    }

    public byte[] inflate(byte[] data) throws IOException {
        return LZFDecoder.decode(data);
    }
}
