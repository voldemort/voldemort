package voldemort.store.readonly.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;

import voldemort.TestUtils;
import voldemort.utils.ByteUtils;


public class ReadOnlyTestUtils {

    /**
     * Determines if two binary files are equal
     * 
     * @param fileA
     * @param fileB
     * @return
     * @throws IOException
     */
    public static boolean areTwoBinaryFilesEqual(File fileA, File fileB) throws IOException {
        // compare file sizes
        if(fileA.length() != fileB.length())
            return false;

        // read and compare bytes pair-wise
        InputStream inputStream1 = new FileInputStream(fileA);
        InputStream inputStream2 = new FileInputStream(fileB);
        int nextByteFromInput1, nextByteFromInput2;
        do {
            nextByteFromInput1 = inputStream1.read();
            nextByteFromInput2 = inputStream2.read();
        } while(nextByteFromInput1 == nextByteFromInput2 && nextByteFromInput1 != -1);
        inputStream1.close();
        inputStream2.close();

        // true only if end of file is reached
        return nextByteFromInput1 == -1;
    }

    /**
     * Un gzip the compressedFile to the given decompressedFile
     * 
     * @param compressedFile
     * @param decompressedFile
     */
    public static void unGunzipFile(String compressedFile, String decompressedFile) {

        byte[] buffer = new byte[1024];
        try {
            FileSystem fs = FileSystem.getLocal(new Configuration());
            FSDataInputStream fileIn = fs.open(new Path(compressedFile));
            GZIPInputStream gZIPInputStream = new GZIPInputStream(fileIn);
            FileOutputStream fileOutputStream = new FileOutputStream(decompressedFile);
            int bytes_read;
            while((bytes_read = gZIPInputStream.read(buffer)) > 0) {
                fileOutputStream.write(buffer, 0, bytes_read);
            }
            gZIPInputStream.close();
            fileOutputStream.close();

        } catch(IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Generate ArrayList<BytesWritable> values from the given key and value
     * string
     * 
     * @param key
     * @param value
     * @param saveKeys boolean that determines the value format
     * @return
     */
    public static ArrayList<BytesWritable> generateValues(String key, String value, boolean saveKeys) {
        byte[] value_1;
        int currentOffset = 0;
        if(saveKeys) {
            value_1 = new byte[4 * ByteUtils.SIZE_OF_INT + ByteUtils.SIZE_OF_BYTE + key.length()
                               + value.length()];
            byte[] randBytes = TestUtils.randomBytes(2 * ByteUtils.SIZE_OF_INT
                                                     + ByteUtils.SIZE_OF_BYTE);
            System.arraycopy(randBytes, 0, value_1, currentOffset, randBytes.length);
            currentOffset += randBytes.length;

            // Write Key Length
            int keyLength = key.length();
            ByteUtils.writeInt(value_1, keyLength, currentOffset);
            currentOffset += ByteUtils.SIZE_OF_INT;

            // Write value length
            int valueLength = value.length();
            ByteUtils.writeInt(value_1, valueLength, currentOffset);
            currentOffset += ByteUtils.SIZE_OF_INT;

            // write key
            System.arraycopy(key.getBytes(), 0, value_1, currentOffset, keyLength);
            currentOffset += keyLength;

            // write value
            System.arraycopy(value.getBytes(), 0, value_1, currentOffset, valueLength);
            currentOffset += valueLength;

        } else {
            value_1 = new byte[2 * ByteUtils.SIZE_OF_INT + value.length()];
            byte[] randBytes = TestUtils.randomBytes(2 * ByteUtils.SIZE_OF_INT);
            System.arraycopy(randBytes, 0, value_1, currentOffset, randBytes.length);
            currentOffset += randBytes.length;

            System.arraycopy(value.getBytes(), 0, value_1, currentOffset, value.length());
            currentOffset += value.length();

        }

        BytesWritable valueBytes = new BytesWritable(value_1);
        ArrayList<BytesWritable> result = new ArrayList<BytesWritable>();
        result.add(valueBytes);
        return result;

    }

}
