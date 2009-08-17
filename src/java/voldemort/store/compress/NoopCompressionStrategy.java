package voldemort.store.compress;

import java.io.IOException;

public class NoopCompressionStrategy implements CompressionStrategy {

    public byte[] deflate(byte[] data) throws IOException {
        return data;
    }

    public byte[] inflate(byte[] data) throws IOException {
        return data;
    }

    public String getType() {
        return "noop";
    }
}
