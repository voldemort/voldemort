package org.h2.compress;

import java.io.IOException;

public class LZFEncoder
{
    // Chunk length is limited by 2-byte length indicator, to 64k
    private static final int MAX_CHUNK_LEN = 0xFFFF;

    // Beyond certain point we won't be able to compress:
    private static final int MIN_BLOCK_TO_COMPRESS = 16;

    private static final int MIN_HASH_SIZE = 256;
    // Not much point in bigger tables, with 8k window
    private static final int MAX_HASH_SIZE = 16384;

    // // Reusable tables

    /**
     * Buffer in which encoded content is stored during processing
     */
    private byte[] _encodeBuffer;    
    
    private int[] _hashTable;
    
    private int _hashModulo;

    public LZFEncoder() { }
    
    /**
     * Method for compressing given input data using LZF encoding and
     * block structure (compatible with lzf command line utility).
     * Result consists of sequence of chunks.
     */
    public byte[] compress(byte[] data) throws IOException
    {
        int left = data.length;
        int inputOffset = 0;
        LZFChunk first = null, curr = null;
        int totalBytes = 0;
        
        while (left > 0) {
            int len = Math.min(left, MAX_CHUNK_LEN);
            LZFChunk chunk = compressChunk(data, inputOffset, len);
            inputOffset += len;
            left -= len;
            if (first == null) {
                first = curr = chunk;
            } else {
                curr.setNext(chunk);
                curr = chunk;
            }
            totalBytes += curr.length();
        }
        // Ok how much we got all in all?
        byte[] result = new byte[totalBytes];
        int ptr = 0;
        for (; first != null; first = first.next()) {
            ptr = first.copyTo(result, ptr);
        }
        return result;
    }

    /**
     * Method for compressing (or not) individual chunks
     */
    private LZFChunk compressChunk(byte[] data, int offset, int len)
    {
        // sanity check: no need to check tiniest of blocks
        if (len < MIN_BLOCK_TO_COMPRESS) {
            return LZFChunk.createNonCompressed(data, offset, len);
        }
        // Ok, then, what's the worst case output buffer length?
        // length indicator for each 32 literals, so:
        int bufferLen = len + ((len + 31) >> 5);
        if (_encodeBuffer == null || _encodeBuffer.length < bufferLen) {
            _encodeBuffer = new byte[bufferLen];
        }
        // And we'll need hash table too
        if (_hashTable == null) {
            initHashTable(len);
        }
        /* And then see if we can compress the block by 2 bytes (since header is
         * 2 bytes longer)
         */
        int compLen = tryCompress(data, offset, offset+len, _encodeBuffer, 0);
        if (compLen > (len-2)) { // nah; just return uncompressed
            return LZFChunk.createNonCompressed(data, offset, len);
        }
        // yes, worth the trouble:
        return LZFChunk.createCompressed(len, _encodeBuffer, 0, compLen);
    }
    
}
