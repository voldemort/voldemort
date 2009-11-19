package voldemort.store.fs;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.PersistenceFailureException;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * A representation of a key/values pairing stored in a file. Includes a
 * checksum for the values.
 * 
 * @author jay
 * 
 */
public class FsKeyAndValues {

    private static final Logger logger = Logger.getLogger(FsKeyAndValues.class);

    private final ByteArray key;
    private final List<Versioned<byte[]>> values;

    public FsKeyAndValues(ByteArray key, Versioned<byte[]>... values) {
        this(key, Arrays.asList(values));
    }

    public FsKeyAndValues(ByteArray key, List<Versioned<byte[]>> values) {
        this.key = key;
        this.values = values;
    }

    public ByteArray getKey() {
        return key;
    }

    public List<Versioned<byte[]>> getValues() {
        return values;
    }

    /**
     * Write the key and values to the given file
     * 
     * @param outputFile The file to write to
     * @throws IOException If writing fails
     */
    public void writeTo(File outputFile) throws IOException {
        int size = 4 /* crc */+ 4 /* key size */+ key.length();
        for(Versioned<byte[]> versioned: values) {
            VectorClock clock = (VectorClock) versioned.getVersion();
            size += 4 /* value size */+ clock.sizeInBytes() + versioned.getValue().length;
        }

        byte[] bytes = new byte[size];
        int offset = 4; // skip crc

        // write the key
        ByteUtils.writeInt(bytes, key.length(), offset);
        offset += 4;
        System.arraycopy(key.get(), 0, bytes, offset, key.length());
        offset += key.length();

        // write the values
        for(Versioned<byte[]> versioned: values) {
            VectorClock clock = (VectorClock) versioned.getVersion();
            byte[] clockBytes = clock.toBytes();
            System.arraycopy(clockBytes, 0, bytes, offset, clockBytes.length);
            offset += clockBytes.length;
            ByteUtils.writeInt(bytes, versioned.getValue().length, offset);
            offset += 4;
            System.arraycopy(versioned.getValue(), 0, bytes, offset, versioned.getValue().length);
            offset += versioned.getValue().length;
        }

        // add the crc32
        CRC32 crc = new CRC32();
        crc.update(bytes, 4, bytes.length - 4);
        ByteUtils.writeInt(bytes, (int) (crc.getValue() & 0xffffffffL), 0);

        // now write all the bytes to the file
        FileOutputStream output;
        try {
            output = new FileOutputStream(outputFile);
        } catch(FileNotFoundException e) {
            // directory does not exist, create it and try again
            outputFile.getParentFile().mkdirs();
            output = new FileOutputStream(outputFile);
        }
        try {
            output.write(bytes);
        } finally {
            try {
                output.close();
            } catch(IOException e) {
                logger.error("Error closing file in write.", e);
            }
        }

    }

    /**
     * Read the key and associated values, or null
     * 
     * @param path The path to read from
     * @return The key and values or null
     * @throws IOException If reading fails
     */
    public static FsKeyAndValues read(File file) throws IOException {
        // check the file size
        long size = file.length();
        if(size == 0)
            return null;
        else if(size > Integer.MAX_VALUE)
            throw new VoldemortException("File " + file + " exceeds maximum file size of "
                                         + Integer.MAX_VALUE + ".");

        // read the bytes
        byte[] bytes = new byte[(int) size];
        InputStream input = new FileInputStream(file);
        try {
            readBuffer(input, bytes);
        } finally {
            try {
                input.close();
            } catch(IOException e) {
                logger.error("Error closing file in read.", e);
            }
        }
        int offset = 0;

        // check the file crc
        CRC32 crc = new CRC32();
        long storedCrc = ByteUtils.readInt(bytes, offset) & 0xffffffffL;
        crc.update(bytes, offset, bytes.length - offset);
        offset += 4;
        if(crc.getValue() == storedCrc)
            throw new PersistenceFailureException("CRC32 for file " + file
                                                  + " did not validate. Expected " + storedCrc
                                                  + " but found " + crc.getValue());

        // read the key and values
        int keySize = ByteUtils.readInt(bytes, offset);
        offset += 4;
        List<Versioned<byte[]>> values = new ArrayList<Versioned<byte[]>>(1);
        byte[] keyBytes = ByteUtils.copy(bytes, offset, offset + keySize);
        offset += keySize;
        while(offset < bytes.length) {
            VectorClock clock = new VectorClock(bytes, offset);
            offset += clock.sizeInBytes();
            int valSize = ByteUtils.readInt(bytes, offset);
            offset += 4;
            byte[] value = ByteUtils.copy(bytes, offset, offset + valSize);
            values.add(new Versioned<byte[]>(value, clock));
            offset += valSize;
        }
        if(values.size() < 1)
            throw new VoldemortException("No values found with key.");

        return new FsKeyAndValues(new ByteArray(keyBytes), values);
    }

    /**
     * Read a buffer of known length from the input stream
     * 
     * @param input The stream to read from
     * @param buffer The buffer to read into
     * @return The buffer, now full
     * @throws IOException If reading fails
     */
    public static byte[] readBuffer(InputStream input, byte[] buffer) throws IOException {
        int read = 0;
        do {
            read = input.read(buffer, read, buffer.length - read);
            if(read < 0)
                throw new EOFException("Unexpected eof while reading file.");
        } while(read < buffer.length);

        return buffer;
    }

}
