package org.h2.compress;

import java.io.IOException;

/**
 * Encoder that handles splitting of input into chunks to encode,
 * calls {@link ChunkEncoder} to compress individual chunks and
 * combines resulting chunks into contiguous output byte array.
 *<p>
 * Code based on H2 Java LZF implementation.
 * 
 * @author tsaloranta@gmail.com
 */
public class LZFEncoder
{
    // Static methods only, no point in instantiating
    private LZFEncoder() { }
    
    /**
     * Method for compressing given input data using LZF encoding and
     * block structure (compatible with lzf command line utility).
     * Result consists of a sequence of chunks.
     */
    public static byte[] encode(byte[] data) throws IOException
    {
        int left = data.length;
        ChunkEncoder enc = new ChunkEncoder(left);
        int chunkLen = Math.min(LZFChunk.MAX_CHUNK_LEN, left);
        LZFChunk first = enc.encodeChunk(data, 0, chunkLen);
        left -= chunkLen;
        // shortcut: if it all fit in, no need to coalesce:
        if (left < 1) {
            return first.getData();
        }
        // otherwise need to get other chunks:
        int resultBytes = first.length();
        int inputOffset = chunkLen;
        LZFChunk last = first;

        do {
            chunkLen = Math.min(left, LZFChunk.MAX_CHUNK_LEN);
            LZFChunk chunk = enc.encodeChunk(data, inputOffset, chunkLen);
            inputOffset += chunkLen;
            left -= chunkLen;
            resultBytes += chunk.length();
            last.setNext(chunk);
            last = chunk;
        } while (left > 0);
        // and then coalesce returns into single contiguous byte array
        byte[] result = new byte[resultBytes];
        int ptr = 0;
        for (; first != null; first = first.next()) {
            ptr = first.copyTo(result, ptr);
        }
        return result;
    }
}
