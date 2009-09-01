package voldemort.store.compress;

import java.io.IOException;

/**
 * Implementations of this interface provide a strategy for compressing and
 * uncompressing data.
 */
public interface CompressionStrategy {

    /**
     * The type of compression performed.
     */
    String getType();

    /**
     * Uncompresses the data. The array received should not be modified, but it
     * may be returned unchanged.
     * 
     * @param data compressed data.
     * @return uncompressed data.
     * @throws IOException if there is an issue during the operation.
     */
    byte[] inflate(byte[] data) throws IOException;

    /**
     * Compresses the data. The array received should not be modified, but it
     * may be returned unchanged.
     * 
     * @param data uncompressed data.
     * @return compressed data.
     * @throws IOException if there is an issue during the operation.
     */
    byte[] deflate(byte[] data) throws IOException;
}
