package voldemort.store.compress;

import java.io.IOException;

public interface CompressionStrategy {

    String getType();

    byte[] inflate(byte[] data) throws IOException;

    byte[] deflate(byte[] data) throws IOException;
}
