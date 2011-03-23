package voldemort.store.readonly.chunk;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.utils.ByteUtils;

import com.google.common.collect.Lists;

public class DataFileChunkSetIteratorTest {

    public class SimpleDataFileChunkSetIterator extends DataFileChunkSetIterator<ByteBuffer> {

        public SimpleDataFileChunkSetIterator(DataFileChunkSet dataFileChunkSet,
                                              boolean coalesceCollided) {
            super(dataFileChunkSet, coalesceCollided);
        }

        @Override
        public ByteBuffer next() {
            if(!hasNext())
                throw new VoldemortException("Reached the end");

            try {
                if(coalesceCollided) {

                    // Read a byte
                    ByteBuffer numKeysBytes = ByteBuffer.allocate(ByteUtils.SIZE_OF_BYTE);
                    getCurrentChunk().read(numKeysBytes, getCurrentOffsetInChunk());
                    int numKeys = numKeysBytes.get(0) & ByteUtils.MASK_11111111;

                    // Read all the collided values
                    ByteBuffer values = ByteBuffer.allocate(numKeys * ByteUtils.SIZE_OF_INT);
                    getCurrentChunk().read(values,
                                           getCurrentOffsetInChunk() + ByteUtils.SIZE_OF_BYTE);

                    // update the offset
                    updateOffset(getCurrentOffsetInChunk() + ByteUtils.SIZE_OF_BYTE
                                 + (numKeys * ByteUtils.SIZE_OF_INT));

                    return values;
                } else {

                    // Read a value
                    ByteBuffer value = ByteBuffer.allocate(ByteUtils.SIZE_OF_INT);
                    getCurrentChunk().read(value, getCurrentOffsetInChunk());

                    updateOffset(getCurrentOffsetInChunk() + ByteUtils.SIZE_OF_INT);

                    return value;
                }
            } catch(IOException e) {
                throw new VoldemortException(e);
            }
        }
    }

    public DataFileChunkSet generateDataFileChunkSet(int numberOfChunks,
                                                     int numberOfEmptyChunks,
                                                     int numberOfEntriesPerChunk,
                                                     int numberOfEntriesPerCollision)
            throws IOException {
        Assert.assertEquals(numberOfEntriesPerChunk % numberOfEntriesPerCollision, 0);

        List<DataFileChunk> dataFiles = Lists.newArrayList();
        List<Integer> dataFileSizes = Lists.newArrayList();

        File tempFolder = TestUtils.createTempDir();
        int currentEntry = 0;

        List<Boolean> isEmpty = Lists.newArrayList();
        for(int chunkId = 0; chunkId < numberOfChunks; chunkId++) {
            isEmpty.add(false);
        }
        for(int chunkId = 0; chunkId < numberOfEmptyChunks; chunkId++) {
            isEmpty.add(true);
        }
        Collections.shuffle(isEmpty);

        for(int chunkId = 0; chunkId < (numberOfChunks + numberOfEmptyChunks); chunkId++) {

            boolean empty = isEmpty.get(chunkId);

            // Generate a file
            File chunkFile = new File(tempFolder, Integer.toString(chunkId));
            chunkFile.createNewFile();

            if(!empty) {
                FileOutputStream stream = new FileOutputStream(chunkFile);

                for(int entryId = 0; entryId < numberOfEntriesPerChunk
                                               / numberOfEntriesPerCollision; entryId++) {

                    ByteBuffer buffer = ByteBuffer.allocate(ByteUtils.SIZE_OF_BYTE
                                                            + numberOfEntriesPerCollision
                                                            * ByteUtils.SIZE_OF_INT);
                    buffer.put((byte) (numberOfEntriesPerCollision & ByteUtils.MASK_11111111));

                    for(int entryInCollision = 0; entryInCollision < numberOfEntriesPerCollision; entryInCollision++) {
                        buffer.putInt(currentEntry);
                        currentEntry++;
                    }
                    stream.write(buffer.array());
                }
                stream.flush();
                stream.close();
            }

            dataFiles.add(new LocalDataFileChunk(new FileInputStream(chunkFile).getChannel()));
            dataFileSizes.add((int) chunkFile.length());
        }

        DataFileChunkSet chunkSet = new DataFileChunkSet(dataFiles, dataFileSizes);
        return chunkSet;
    }

    @Test
    public void testIterator() throws IOException {
        // 1) Test with empty chunk files
        testIterator(0, 100, 1);

        // 2) Test with one chunk file
        testIterator(1, 100, 1);

        // 3) Test with two chunk files
        testIterator(2, 100, 1);

        // 4) Test with two chunk files
        testIterator(10, 100, 1);

        // 5) Test with collisions
        testIterator(1, 100, 2);

        // 6) Test with collisions
        testIterator(2, 100, 2);

        // 7) Test with collisions
        testIterator(10, 100, 5);

        // 8) Test with collided entries + empty chunk files
        testIteratorWithCollidedEntry(0, 100, 1);

        // 9) Test with collided entries
        testIteratorWithCollidedEntry(1, 100, 1);

        // 10) Test with collided entries
        testIteratorWithCollidedEntry(1, 100, 2);

        // 11) Test with collided entries
        testIteratorWithCollidedEntry(1, 100, 5);

        // 12) Test with collided entries
        testIteratorWithCollidedEntry(10, 100, 1);

        // 13) Test with collided entries
        testIteratorWithCollidedEntry(10, 100, 2);

        // 14) Test with collided entries
        testIteratorWithCollidedEntry(10, 100, 5);

    }

    public void testIterator(int numChunks, int numEntriesPerChunks, int numEntriesPerCollision)
            throws IOException {
        // 1) Test with empty chunk files
        SimpleDataFileChunkSetIterator iterator = new SimpleDataFileChunkSetIterator(generateDataFileChunkSet(numChunks,
                                                                                                              10,
                                                                                                              numEntriesPerChunks,
                                                                                                              numEntriesPerCollision),
                                                                                     false);

        if(numChunks * numEntriesPerChunks == 0) {
            assertEquals(iterator.hasNext(), false);
        } else {
            for(int entry = 0; entry < numChunks * numEntriesPerChunks; entry++) {
                assertEquals(iterator.hasNext(), true);
                ByteBuffer buffer = iterator.next();
                assertEquals(buffer.getInt(0), entry);
            }
        }
    }

    public void testIteratorWithCollidedEntry(int numChunks,
                                              int numEntriesPerChunks,
                                              int numEntriesPerCollision) throws IOException {
        // 1) Test with empty chunk files
        SimpleDataFileChunkSetIterator iterator = new SimpleDataFileChunkSetIterator(generateDataFileChunkSet(numChunks,
                                                                                                              10,
                                                                                                              numEntriesPerChunks,
                                                                                                              numEntriesPerCollision),
                                                                                     true);

        if(numChunks * numEntriesPerChunks == 0) {
            assertEquals(iterator.hasNext(), false);
        } else {
            int entry = 0;
            for(int collidedEntry = 0; collidedEntry < numChunks
                                                       * (numEntriesPerChunks / numEntriesPerCollision); collidedEntry++) {
                assertEquals(iterator.hasNext(), true);
                ByteBuffer buffer = iterator.next();

                // Read number
                assertEquals(buffer.array().length, numEntriesPerCollision * ByteUtils.SIZE_OF_INT);

                for(int index = 0; index < numEntriesPerCollision; index++) {
                    assertEquals(buffer.getInt(ByteUtils.SIZE_OF_INT * index), entry++);
                }
            }
        }
    }

}
