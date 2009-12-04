package org.h2.compress;

import java.io.*;

/**
 * Simple non-streaming compressor that can compress and uncompress
 * blocks of varying sizes efficiently.
 */
public final class LZFCodec
{
    private static final int MAX_LITERAL = 1 << 5; // 32
    private static final int MAX_OFF = 1 << 13; // 8k
    private static final int MAX_REF = (1 << 8) + (1 << 3); // 264

    private static final int MIN_HASH_SIZE = 256;
    // Not much point in bigger tables, with 8k window
    private static final int MAX_HASH_SIZE = 16384;
    
    // Chunk length is limited by 2-byte length indicator, to 64k
    private static final int MAX_CHUNK_LEN = 0xFFFF;

    // Beyond certain point we won't be able to compress:
    private static final int MIN_BLOCK_TO_COMPRESS = 16;

    final static byte BYTE_NULL = 0;    

    // // Reusable tables

    private byte[] _compressBuffer;
    
    private int[] _hashTable;
    
    private int _hashModulo;
    
    public LZFCodec() { }

    /*
    ******************************************************
    * Public entry methods
    ******************************************************
    */

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
     * Method for decompressing whole input data, which encoded in LZF
     * block structure (compatible with lzf command line utility),
     * and can consist of any number of blocks
     */
    public byte[] decompress(byte[] data) throws IOException
    {
        /* First: let's calculate actual size, so we can allocate
         * exact result size. Also useful for basic sanity checking;
         * so that after call we know header structure is not corrupt
         * (to the degree that lengths etc seem valid)
         */
        byte[] result = new byte[calculateUncompressedSize(data)];
        int inPtr = 0;
        int outPtr = 0;

        while (inPtr < (data.length - 1)) { // -1 to offset possible end marker
            inPtr += 2; // skip 'ZV' marker
            int type = data[inPtr++];
            int len = uint16(data, inPtr);
            inPtr += 2;
            if (type == LZFChunk.BLOCK_TYPE_NON_COMPRESSED) { // uncompressed
                System.arraycopy(data, inPtr, result, outPtr, len);
                outPtr += len;
            } else { // compressed
                int uncompLen = uint16(data, inPtr);
                inPtr += 2;
                decompressBlock(data, inPtr, result, outPtr, outPtr+uncompLen);
                outPtr += uncompLen;
            }
            inPtr += len;
        }
        return result;
    }

    /*
     ******************************************************
     * Compression helper methods
     ******************************************************
     */

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
        if (_compressBuffer == null || _compressBuffer.length < bufferLen) {
            _compressBuffer = new byte[bufferLen];
        }
        // And we'll need hash table too
        if (_hashTable == null) {
            initHashTable(len);
        }
        /* And then see if we can compress the block by 2 bytes (since header is
         * 2 bytes longer)
         */
        int compLen = tryCompress(data, offset, offset+len, _compressBuffer, 0);
        if (compLen > (len-2)) { // nah; just return uncompressed
            return LZFChunk.createNonCompressed(data, offset, len);
        }
        // yes, worth the trouble:
        return LZFChunk.createCompressed(len, _compressBuffer, 0, compLen);
    }

    private int first(byte[] in, int inPos) {
        return (in[inPos] << 8) + (in[inPos + 1] & 255);
    }

    private static int next(int v, byte[] in, int inPos) {
        return (v << 8) + (in[inPos + 2] & 255);
    }


    private int hash(int h) {
        // or 184117; but this seems to give better hashing?
        return ((h * 57321) >> 9) & _hashModulo;
        // original lzf-c.c used this:
        //return (((h ^ (h << 5)) >> (24 - HLOG) - h*5) & _hashModulo;
        // but that didn't seem to provide better matches
    }
    
    public int tryCompress(byte[] in, int inPos, int inEnd, byte[] out, int outPos)
    {
        int literals = 0;
        outPos++;
        int hash = first(in, 0);
        inEnd -= 4;
        while (inPos < inEnd) {
            byte p2 = in[inPos + 2];
            // next
            hash = (hash << 8) + (p2 & 255);
            int off = hash(hash);
            int ref = _hashTable[off];
            _hashTable[off] = inPos;
            if (ref < inPos
                        && ref > 0
                        && (off = inPos - ref - 1) < MAX_OFF
                        && in[ref + 2] == p2
                        && in[ref + 1] == (byte) (hash >> 8)
                        && in[ref] == (byte) (hash >> 16)) {
                // match
                int maxLen = inEnd - inPos + 2;
                if (maxLen > MAX_REF) {
                    maxLen = MAX_REF;
                }
                if (literals == 0) {
                    outPos--;
                } else {
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                }
                int len = 3;
                while (len < maxLen && in[ref + len] == in[inPos + len]) {
                    len++;
                }
                len -= 2;
                if (len < 7) {
                    out[outPos++] = (byte) ((off >> 8) + (len << 5));
                } else {
                    out[outPos++] = (byte) ((off >> 8) + (7 << 5));
                    out[outPos++] = (byte) (len - 7);
                }
                out[outPos++] = (byte) off;
                outPos++;
                inPos += len;
                hash = first(in, inPos);
                hash = next(hash, in, inPos);
                _hashTable[hash(hash)] = inPos++;
                hash = next(hash, in, inPos);
                _hashTable[hash(hash)] = inPos++;
            } else {
                out[outPos++] = in[inPos++];
                literals++;
                if (literals == MAX_LITERAL) {
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                    outPos++;
                }
            }
        }
        inEnd += 4;
        while (inPos < inEnd) {
            out[outPos++] = in[inPos++];
            literals++;
            if (literals == MAX_LITERAL) {
                out[outPos - literals - 1] = (byte) (literals - 1);
                literals = 0;
                outPos++;
            }
        }
        out[outPos - literals - 1] = (byte) (literals - 1);
        if (literals == 0) {
            outPos--;
        }
        return outPos;
    }

    /**
     * Helper method that allocates hash table of suitable size,
     * based on size of first chunk (which will be no smaller
     * than any of the chunks that follows)
     */
    private void initHashTable(int chunkSize)
    {
        int hashLen = calcHashLen(chunkSize);
        _hashTable = new int[hashLen];
        _hashModulo = hashLen-1;
    }
    
    private static int calcHashLen(int chunkSize)
    {
        // in general try get hash table size of 2x input size
        chunkSize += chunkSize;
        // but no larger than max size:
        if (chunkSize >= MAX_HASH_SIZE) {
            return MAX_HASH_SIZE;
        }
        // otherwise just need to round up to nearest 2x
        int hashLen = MIN_HASH_SIZE;
        while (hashLen < chunkSize) {
            hashLen += hashLen;
        }
        return hashLen;
    }
    
    /*
     ******************************************************
     * Decompression helper methods
     ******************************************************
     */

    /**
     * Main decompress method for individual blocks/chunks.
     */
    public static void decompressBlock(byte[] in, int inPos, byte[] out, int outPos, int outEnd)
        throws IOException
    {
        do {
            int ctrl = in[inPos++] & 255;
            if (ctrl < MAX_LITERAL) { // literal run
                ctrl += inPos;
                do {
                    out[outPos++] = in[inPos];
                } while (inPos++ < ctrl);
            } else {
                // back reference
                int len = ctrl >> 5;
                ctrl = -((ctrl & 0x1f) << 8) - 1;
                if (len == 7) {
                    len += in[inPos++] & 255;
                }
                ctrl -= in[inPos++] & 255;
                len += outPos + 2;
                out[outPos] = out[outPos++ + ctrl];
                out[outPos] = out[outPos++ + ctrl];
                while (outPos < len - 8) {
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                }
                while (outPos < len) {
                    out[outPos] = out[outPos++ + ctrl];
                }
            }
        } while (outPos < outEnd);

        // sanity check to guard against corrupt data:
        if (outPos != outEnd) throw new IOException("Corrupt data: overrun in decompress, input offset "+inPos+", output offset "+outPos);
    }

    private int calculateUncompressedSize(byte[] data) throws IOException
    {
        int uncompressedSize = 0;
        int ptr = 0;
        int blockNr = 0;

        while (ptr < data.length) {
            // can use optional end marker
            if (ptr == (data.length + 1) && data[ptr] == BYTE_NULL) {
                ++ptr; // so that we'll be at end
                break;
            }
            // simpler to handle bounds checks by catching exception here...
            try {
                if (data[ptr] != LZFChunk.BYTE_Z || data[ptr+1] != LZFChunk.BYTE_V) {
                    throw new IOException("Corrupt input data, block #"+blockNr+" (at offset "+ptr+"): did not start with 'ZV' signature bytes");
                }
                int type = (int) data[ptr+2];
                int blockLen = uint16(data, ptr+3);
                if (type == LZFChunk.BLOCK_TYPE_NON_COMPRESSED) { // uncompressed
                    ptr += 5;
                    uncompressedSize += blockLen;
                } else if (type == LZFChunk.BLOCK_TYPE_COMPRESSED) { // compressed
                    uncompressedSize += uint16(data, ptr+5);
                    ptr += 7;
                } else { // unknown... CRC-32 would be 2, but that's not implemented by cli tool
                    throw new IOException("Corrupt input data, block #"+blockNr+" (at offset "+ptr+"): unrecognized block type "+(type & 0xFF));
                }
                ptr += blockLen;
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IOException("Corrupt input data, block #"+blockNr+" (at offset "+ptr+"): truncated block header");
            }
            ++blockNr;
        }
        // one more sanity check:
        if (ptr != data.length) {
            throw new IOException("Corrupt input data: block #"+blockNr+" extends "+(data.length - ptr)+" beyond end of input");
        }
        return uncompressedSize;
    }

    private static int uint16(byte[] data, int ptr)
    {
        return ((data[ptr] & 0xFF) << 8) + (data[ptr+1] & 0xFF);
    }    

}
