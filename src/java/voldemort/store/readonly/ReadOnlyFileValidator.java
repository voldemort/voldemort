package voldemort.store.readonly;

import voldemort.utils.ByteUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * This class is used to iterate over index and data files to validate their integrity.
 *
 * N.B.: This tool only works with the Read-Only V2 format.
 *
 * May you never need it.
 */
public class ReadOnlyFileValidator {
    public static void main(String[] args) {
        if (args.length < 1) {
            printInfo("Please include the following parameters: data_path [-v]");
            printInfo("");
            printInfo("data_path: Data directory for a store/version, or individual index or data file.");
            printInfo("-v:        Activates the verbose mode (optional, default: off).");
            printInfo("");
            printDisclaimers();
            System.exit(1);
        }

        String pathString = args[0];
        boolean verbose = (args.length > 1 && args[1].equals("-v"));

        File path = new File(pathString);

        ReadOnlyFileValidator validator = new ReadOnlyFileValidator();

        try {
            if (path.isDirectory()) {
                printInfo("Running in directory mode.");
                validator.validateDirectory(path, verbose);
            } else if (path.getName().endsWith(ReadOnlyUtils.INDEX_FILE_EXTENSION)) {
                printInfo("Running in single index file mode.");
                validator.validateIndexFile(path, verbose);
            } else if (path.getName().endsWith(ReadOnlyUtils.DATA_FILE_EXTENSION)) {
                printInfo("Running in single data file mode.");
                validator.validateDataFile(path, verbose);
            } else {
                printInfo("The path specified must be a directory OR a file with either a " +
                          ReadOnlyUtils.INDEX_FILE_EXTENSION + " or " +
                          ReadOnlyUtils.DATA_FILE_EXTENSION + " extension.");
                System.exit(1);
            }
        } catch (Exception e) {
            printError("ReadOnlyFileValidator threw an exception!");
            e.printStackTrace(System.err);
            System.exit(1);
        }

        printInfo("");
        printDisclaimers();
        printInfo("");
        printInfo("ReadOnlyFileValidator finished running successfully.");
        System.exit(0);
    }

    /**
     * Print an error message if verbose is true.
     *
     * The current implementation prints to STDERR.
     *
     * This abstraction is in place in case we want to refactor the tool to use log4j in the future.
     *
     * @param verbose message will be printed only if this parameter is true
     * @param message to print
     */
    private static void printError(boolean verbose, String message) {
        if (verbose) {
            printError(message);
        }
    }

    private static void printError(String message) {
        System.err.println(message);
    }

    /**
     * Print an info message if verbose is true.
     *
     * The current implementation prints to STDOUT.
     *
     * This abstraction is in place in case we want to refactor the tool to use log4j in the future.
     *
     * @param verbose message will be printed only if this parameter is true
     * @param message to print
     */
    private static void printInfo(boolean verbose, String message) {
        if (verbose) {
            printInfo(message);
        }
    }

    private static void printInfo(String message) {
        System.err.println(message);
    }

    private void printProblemsDetected(int problemsDetected) {
        printInfo("\t -> " + problemsDetected + " problems detected.");
    }

    private static void printDisclaimers() {
        printInfo("Disclaimers:");
        printInfo("- This tool only works with Voldemort Read-Only V2 formatted files.");
        printInfo("- This tool does NOT rely on the checksum in the .metadata file.");
        printInfo("- This tool only inspects the structural integrity of index and data files, and whether they match together.");
        printInfo("- If a problem is detected, you're in trouble for sure, but if no problems are detected, that doesn't mean an unsupported problem didn't slip through undetected (i.e.: false negatives are possible but false positives are not).");
    }

    public void validateDirectory(File path, boolean verbose) throws IOException {
        File[] files = path.listFiles();
        Arrays.sort(files);
        int problemsDetectedInIndexFiles = 0,
            problemsDetectedInDataFiles = 0,
            problemsDetectedInIndexAndDataFilesMatch = 0;
        for (File file: files) {
            if (file.length() == 0) {
                if (verbose) {
                    printInfo("Skipping empty file: " + file.getName());
                }
            } else if (file.getName().endsWith(ReadOnlyUtils.INDEX_FILE_EXTENSION)) {
                problemsDetectedInIndexFiles += validateIndexFile(file, verbose);
                problemsDetectedInIndexAndDataFilesMatch += validateIndexAndDataFileMatch(file, verbose);
            } else if (file.getName().endsWith(ReadOnlyUtils.DATA_FILE_EXTENSION)) {
                problemsDetectedInDataFiles += validateDataFile(file, verbose);
            } else {
                printInfo("Skipping file: " + file.getName());
            }
        }

        printInfo("");

        if (problemsDetectedInIndexFiles == 0 &&
                problemsDetectedInDataFiles == 0 &&
                problemsDetectedInIndexAndDataFilesMatch == 0 ) {
            printInfo("No problems detected in any of the files in this directory.");
        } else {
            printError("Number of problems detected in index files: " + problemsDetectedInIndexFiles);
            printError("Number of problems detected in data files: " + problemsDetectedInDataFiles);
            printError("Number of problems detected in the matching of index and data files: " +
                       problemsDetectedInIndexAndDataFilesMatch);
        }
    }

    public int validateIndexFile(File indexFile, boolean verbose) throws IOException {
        System.out.print("Examining: " + indexFile.getName() + "   "); // Note: print instead of println
        printInfo(verbose, "");

        FileInputStream fileInputStream = new FileInputStream(indexFile);
        byte[] currentKeyHash = new byte[8],
               currentKeyOffset = new byte[ReadOnlyUtils.POSITION_SIZE],
               previousKeyHash = null,
               previousKeyOffset = null;
        int keysRead = 0,
            keyOffset,
            problemsDetected = 0;
        while (fileInputStream.available() > 0) {
            keysRead++;

            fileInputStream.read(currentKeyHash);
            fileInputStream.read(currentKeyOffset);

            printInfo(verbose, "Key #" + keysRead +
                               ", Hash: " + ByteUtils.toHexString(currentKeyHash) +
                               ", Offset: " + ByteBuffer.wrap(currentKeyOffset).getInt());

            keyOffset = ByteBuffer.wrap(currentKeyOffset).getInt();
            if (keyOffset < 0) {
                printError(verbose, "Key #" + keysRead + " has a negative offset ( " + keyOffset + ").");
                problemsDetected++;
            }

            if (previousKeyHash != null) {
                problemsDetected += verifyByteArraysAreStrictlyIncreasing(previousKeyHash,
                                                                          currentKeyHash,
                                                                          "hashes",
                                                                          keysRead,
                                                                          verbose);
                problemsDetected += verifyByteArraysAreStrictlyIncreasing(previousKeyOffset,
                                                                          currentKeyOffset,
                                                                          "offsets",
                                                                          keysRead,
                                                                          verbose);
            }

            previousKeyHash = ByteUtils.copy(currentKeyHash, 0, currentKeyHash.length);
            previousKeyOffset = ByteUtils.copy(currentKeyOffset, 0, currentKeyOffset.length);
        }

        printProblemsDetected(problemsDetected);

        return problemsDetected;
    }

    private int verifyByteArraysAreStrictlyIncreasing(byte[] previousByteArray,
                                                       byte[] currentByteArray,
                                                       String description,
                                                       int keysRead,
                                                       boolean verbose) {
        int cmp = ByteUtils.compare(previousByteArray, currentByteArray);

        if (cmp < 0) {
            // This is the expected case. The index file appears to be well-formed so far.
            return 0;
        } else if (cmp == 0) {
            // The same key hash or offset appeared twice in a row. Unexpected!
            printError(verbose, "We found two identical consecutive key " + description + "! " +
                                keysRead + " keys read so far.");
        } else {
            // The index file is not properly sorted. Unexpected!
            printError(verbose, "We found two consecutive key " + description +
                                " which are not in increasing order. " + keysRead + " keys read so far.");
        }

        return 1;
    }

    public int validateDataFile(File dataFile, boolean verbose) throws IOException {
        System.out.print("Examining: " + dataFile.getName() + "    "); // Note: print instead of println
        printInfo(verbose, "");

        FileInputStream fileInputStream = new FileInputStream(dataFile);
        byte[] currentNumberOfKeys = new byte[2],
               currentKeySize = new byte[ByteUtils.SIZE_OF_INT],
               currentValueSize = new byte[ByteUtils.SIZE_OF_INT];
        int keysRead = 0,
            problemsDetected = 0,
            keySize,
            valueSize;
        short numberOfKeys = 0;
        while (fileInputStream.available() > 0) {
            keysRead++;

            if (numberOfKeys == 0) {
                fileInputStream.read(currentNumberOfKeys);
                numberOfKeys = ByteBuffer.wrap(currentNumberOfKeys).getShort();
                if (numberOfKeys > 1) {
                    // Not a problem, hence why we don't increment problemsDetected, but interesting nonetheless
                    printInfo(verbose, "Hash collision: " + numberOfKeys + " with the same hash.");
                } else if (numberOfKeys < 1) {
                    printError(verbose, "Bad number of keys (" + numberOfKeys + ") found in data file entry!");
                    problemsDetected++;
                }
            }

            numberOfKeys--;
            fileInputStream.read(currentKeySize);
            fileInputStream.read(currentValueSize);

            keySize = ByteBuffer.wrap(currentKeySize).getInt();
            valueSize = ByteBuffer.wrap(currentValueSize).getInt();

            printInfo(verbose, "Key #" + keysRead +
                               ", Key Size: " + keySize +
                               ", Value Size: " + valueSize);

            if (keySize == 0) {
                printError(verbose, "Key size 0!");
                problemsDetected++;
            } else if (keySize < 0) {
                printError(verbose, "Key size is negative!");
                problemsDetected++;
            }

            if (valueSize == 0) {
                printError(verbose, "Value size 0!");
                problemsDetected++;
            } else if (valueSize < 0) {
                printError(verbose, "Value size is negative!");
                problemsDetected++;
            }

            fileInputStream.skip(keySize + valueSize);
        }

        printProblemsDetected(problemsDetected);

        return problemsDetected;
    }

    public int validateIndexAndDataFileMatch(File indexFile, boolean verbose) throws IOException {
        String partitionAndChunkName = indexFile.getName().substring(0, indexFile.getName().indexOf('.'));
        File dataFile = new File(indexFile.getParent(), partitionAndChunkName + ReadOnlyUtils.DATA_FILE_EXTENSION);

        System.out.print("Examining that " + partitionAndChunkName + " matches"); // Note: print instead of println
        printInfo(verbose, "");

        FileInputStream indexFileInputStream = new FileInputStream(indexFile);
        FileChannel dataFileChannel = new FileInputStream(dataFile).getChannel();
        byte[] currentKeyHashFromIndexFile = new byte[8],
               currentKeyHashFromDataFile = new byte[8],
               currentKeyOffsetArray = new byte[ReadOnlyUtils.POSITION_SIZE],
               currentKey,
               currentValue;
        ByteBuffer currentNumberOfKeysBuffer = ByteBuffer.allocate(ByteUtils.SIZE_OF_SHORT),
                   currentKeySizeBuffer = ByteBuffer.allocate(ByteUtils.SIZE_OF_INT),
                   currentValueSizeBuffer = ByteBuffer.allocate(ByteUtils.SIZE_OF_INT),
                   currentKeyBuffer,
                   currentValueBuffer;
        int keysHashRead = 0,
            keysRead = 0,
            problemsDetected = 0,
            currentKeyHashOffset,
            currentNumberOfKeys,
            currentKeySize,
            currentValueSize;
        long currentPositionInDataFile = 0;
        while (indexFileInputStream.available() > 0) {
            keysHashRead++;

            // Read next record in index file
            indexFileInputStream.read(currentKeyHashFromIndexFile);
            indexFileInputStream.read(currentKeyOffsetArray);
            currentKeyHashOffset = ByteBuffer.wrap(currentKeyOffsetArray).getInt();

            // Seek to corresponding position in data file (if necessary)
            if (currentKeyHashOffset != currentPositionInDataFile) {
                printError(verbose, "There seems to be a gap in the data file. " +
                                    "Key Hash #" + keysHashRead +
                                    ", Offset from index file: " + currentKeyHashOffset +
                                    ", Current position in data file: " + currentPositionInDataFile);
                problemsDetected++;
                currentPositionInDataFile = currentKeyHashOffset;
            }

            try { // If the index and data files are mismatched, there's a chance the code below will throw.
                currentPositionInDataFile += read(dataFileChannel, currentNumberOfKeysBuffer, currentPositionInDataFile);
                currentNumberOfKeys = currentNumberOfKeysBuffer.getShort(0);

                while (currentNumberOfKeys > 0) {
                    keysRead++;
                    // Read key size
                    currentPositionInDataFile += read(dataFileChannel, currentKeySizeBuffer, currentPositionInDataFile);
                    currentKeySize = currentKeySizeBuffer.getInt(0);
                    // Read value size
                    currentPositionInDataFile += read(dataFileChannel, currentValueSizeBuffer, currentPositionInDataFile);
                    currentValueSize = currentValueSizeBuffer.getInt(0);
                    // Read key
                    currentKeyBuffer = ByteBuffer.allocate(currentKeySize);
                    currentPositionInDataFile += read(dataFileChannel, currentKeyBuffer, currentPositionInDataFile);
                    currentKey = currentKeyBuffer.array();
                    // Read value
                    currentValueBuffer = ByteBuffer.allocate(currentValueSize);
                    currentPositionInDataFile += read(dataFileChannel, currentValueBuffer, currentPositionInDataFile);
                    currentValue = currentValueBuffer.array(); // currently not used for anything...

                    // Convert key from data file to a hash equivalent to what should have been found in the index file
                    currentKeyHashFromDataFile = ByteUtils.copy(ByteUtils.md5(currentKey), 0, 2 * ByteUtils.SIZE_OF_INT);
                    if (ByteUtils.compare(currentKeyHashFromIndexFile, currentKeyHashFromDataFile) != 0) {
                        printError(verbose, "Key hash found in index file (" + ByteUtils.toHexString(currentKeyHashFromIndexFile) +
                                            ") does not match the hash of the key found in the data file (" +
                                            ByteUtils.toHexString(currentKeyHashFromIndexFile) + ")");
                        problemsDetected++;
                    }

                    currentNumberOfKeys--;

                    printInfo(verbose, "Key Hash #" + keysHashRead +
                                       ", Key Hash: " + ByteUtils.toHexString(currentKeyHashFromIndexFile) +
                                       ", Keys remaining for this hash: " + currentNumberOfKeys +
                                       ", Key #" + keysRead +
                                       ", Key Size: " + currentKeySize +
                                       ", Value Size: " + currentValueSize +
                                       ", Data File Offset: " + currentKeyHashOffset);
                }
            } catch (Exception e) {
                problemsDetected++;
                if (verbose) {
                    printError(verbose, "Caught an exception while trying to read a data file.");
                    e.printStackTrace(System.err);
                }
            }
        }

        printProblemsDetected(problemsDetected);

        return problemsDetected;
    }

    private int read(FileChannel fileChannel, ByteBuffer byteBuffer, long currentPosition) throws IOException {
        int expectedNumberOfBytesRead = byteBuffer.array().length;
        if (fileChannel.size() < currentPosition + expectedNumberOfBytesRead) {
            throw new IOException("Cannot attempt to read a fileChannel at a position (" + currentPosition +
                                  ") beyond the end-of-stream.");
        }
        int bytesRead = fileChannel.read(byteBuffer, currentPosition);
        if (bytesRead < 0) {
            throw new IOException("Attempted to read a fileChannel at a position (" + currentPosition +
                                  ") beyond the end-of-stream.");
        } else if (bytesRead != expectedNumberOfBytesRead) {
            throw new IOException("Expected to read " + expectedNumberOfBytesRead +
                                  " bytes from a FileChannel but was only able to read " + bytesRead + " bytes.");
        }
        return bytesRead;
    }
}
