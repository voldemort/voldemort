package voldemort.store.readonly;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import voldemort.utils.ByteUtils;
import voldemort.utils.EventThrottler;
import voldemort.utils.Pair;


/**
 * ReadOnlyFileDiffer is used to compare between different versions of specific store and give the number of how many
 * records are added/deleted/updated/same.
 *
 * N.B.: This tool only works with the Read-Only V2 format.
 */
public class ReadOnlyFileDiffer {
    public final static int ADD = 0;
    public final static int DELETE = 1;
    public final static int UPDATE = 2;
    public final static int SAME = 3;

    public static void main(String[] args) {
        //Initialize options of command.
        Options options = new Options();
        Option pathOption = new Option("p", "path", true, "Path of Voldemort readonly data dir");
        pathOption.setRequired(true);
        Option storeOption = new Option("s", "store", true, "Store name");
        storeOption.setRequired(true);
        Option oldOption = new Option("o", "oldVersion", true, "Old version number");
        oldOption.setRequired(true);
        Option newOption = new Option("n", "newVersion", true, "New version number");
        newOption.setRequired(true);
        options.addOption(pathOption);
        options.addOption(storeOption);
        options.addOption(oldOption);
        options.addOption(newOption);
        options.addOption("r", "rate", true, "Max rate of reading bytes from disk per second.");
        options.addOption("h", "help", false, "Print usage");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
            //Print help information.
            if (cmd.hasOption("h")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("ReadOnlyFileDiffer", options);
                return;
            }

            String path = cmd.getOptionValue("p");
            String store = cmd.getOptionValue("s");
            int oldVersion = Integer.valueOf(cmd.getOptionValue("o"));
            int newVersion = Integer.valueOf(cmd.getOptionValue("n"));
            int rate = Integer.valueOf(cmd.getOptionValue("r", "-1"));

            ReadOnlyFileDiffer analyzer = new ReadOnlyFileDiffer();

            System.out.println(
                "Start comparing for store:" + store + " between version-" + oldVersion + " and version-" + newVersion);

            long[] diffResults = analyzer.compareForStore(path, store, oldVersion, newVersion, rate);

            System.out.println(
                "Diff of store" + store + ". Added:" + diffResults[ADD] + " Deleted:" + diffResults[DELETE]
                    + " Updated:" + diffResults[UPDATE] + " Same:" + diffResults[SAME]);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }

    /**
     * Compare all the records in given store between the two different versions.
     *
     * @param path Path of Voldemort readonly data directory.
     * @param store Readonly Store name.
     * @param oldVersion old version number.
     * @param newVersion new version number.
     * @return result of difference. Including number of added, deleted, updated and the same.
     */
    public long[] compareForStore(String path, String store, int oldVersion, int newVersion, int rate) {
        long[] diffResults = new long[4];
        File oldVersionDir = getVersionDir(path, store, oldVersion);
        File newVersionDir = getVersionDir(path, store, newVersion);
        HashSet<String> OldPartitions = getPartitionPrefixes(oldVersionDir);
        HashSet<String> newPartitions = getPartitionPrefixes(newVersionDir);
        ArrayList<String> comparablePartitions = new ArrayList<String>();
        ByteArrayComparator comparator = new ByteArrayComparator();

        //Only compare common partitions which exist in both versions.
        for (String partition : OldPartitions) {
            if (newPartitions.contains(partition)) {
                comparablePartitions.add(partition);
            }
        }

        for (String partition : comparablePartitions) {
            long[] results = comparePartition(comparator, new ReadOnlyRecordIterator(oldVersionDir, partition, rate),
                new ReadOnlyRecordIterator(newVersionDir, partition, rate));
            diffResults[ADD] += results[ADD];
            diffResults[DELETE] += results[DELETE];
            diffResults[UPDATE] += results[UPDATE];
            diffResults[SAME] += results[SAME];
        }
        return diffResults;
    }

    /**
     * Compare the records in one partition between two different versions.
     *
     * @param oldIter Records iterator of old version partition.
     * @param newIter Records iterator of new version partition.
     * @return result of difference. Including number of added, deleted, updated and the same.
     */
    protected long[] comparePartition(ByteArrayComparator comparator, ReadOnlyRecordIterator oldIter,
        ReadOnlyRecordIterator newIter) {
        long[] results = new long[4];
        byte[] oldIndex;
        byte[] newIndex;

        oldIndex = oldIter.next();
        newIndex = newIter.next();

        while (oldIndex != null && newIndex != null) {
            int result = comparator.compare(oldIndex, newIndex);
            if (result == 0) {
                //Same hash of keys. Compare keys and values more.
                compareRecordsWithSameHash(comparator, oldIter.readRecords(), newIter.readRecords(), results);
                oldIndex = oldIter.next();
                newIndex = newIter.next();
            } else if (result > 0) {
                //old hash> new hash.
                results[ADD]++;
                newIndex = newIter.next();
            } else {
                //old hash < new hash.
                results[DELETE]++;
                oldIndex = oldIter.next();
            }
        }

        while (oldIndex != null) {
            results[DELETE]++;
            oldIndex = oldIter.next();
        }

        while (newIndex != null) {
            results[ADD]++;
            newIndex = newIter.next();
        }
        return results;
    }

    /**
     * Compare the keys and values one by one between two different versions.
     */
    private void compareRecordsWithSameHash(ByteArrayComparator comparator, Pair<byte[], byte[]>[] oldRecords,
        Pair<byte[], byte[]>[] newRecords, long[] results) {
        int numOldRecords = oldRecords.length;
        int numNewRecords = newRecords.length;
        // These records all have the same hash value, thus maybe there is not fixed order here. \
        // Just compare them one by one as the number of records should be very small.
        for (Pair<byte[], byte[]> oldPRecord : oldRecords) {
            for (Pair<byte[], byte[]> newRecord : newRecords) {
                if (comparator.compare(oldPRecord.getFirst(), newRecord.getFirst()) == 0) {
                    numOldRecords--;
                    numNewRecords--;
                    if (comparator.compare(oldPRecord.getSecond(), newRecord.getSecond()) == 0) {
                        results[SAME]++;
                    } else {
                        results[UPDATE]++;
                    }
                }
            }
        }
        results[ADD] += numNewRecords;
        results[DELETE] += numOldRecords;
    }

    static class ByteArrayComparator implements Comparator<byte[]> {

        @Override
        public int compare(byte[] o1, byte[] o2) {
            int i = 0;
            int j = 0;
            for (; i < o1.length && j < o2.length; i++, j++) {
                if (o1[i] < o2[i]) {
                    return -1;
                } else if (o1[i] > o2[j]) {
                    return 1;
                }
            }
            if (i < o1.length) {
                return 1;
            } else if (j < o2.length) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    /**
     * This class is used to compare between chunk files. Smaller chunk number means smaller when comparing.
     */
    static class ReadOnlyFileNameComparator implements Comparator<String> {

        @Override
        public int compare(String o1, String o2) {
            int chunk1 = Integer.valueOf(o1.substring(o1.lastIndexOf('_') + 1));
            int chunk2 = Integer.valueOf(o2.substring(o2.lastIndexOf('_') + 1));
            return chunk1 - chunk2;
        }
    }

    /**
     * Iterator of records in one partition. No matter how many chunk files in this partition,
     * iterator could get next record correctly in this partition.
     */
    static class ReadOnlyRecordIterator implements Iterator<byte[]> {
        private Iterator<String> fileNameIter;
        private FileInputStream indexInputStream;
        private FileInputStream dataInputStream;
        private EventThrottler throttler;
        private byte[] hash = new byte[ByteUtils.SIZE_OF_LONG];
        private byte[] position = new byte[ReadOnlyUtils.POSITION_SIZE];
        private byte[] numKeysBytes = new byte[ByteUtils.SIZE_OF_SHORT];
        private byte[] keySizeBytes = new byte[ByteUtils.SIZE_OF_INT];
        private byte[] valueSizeBytes = new byte[ByteUtils.SIZE_OF_INT];

        public ReadOnlyRecordIterator(File dir, String prefix) {
            this(dir, prefix, -1);
        }

        public ReadOnlyRecordIterator(File dir, String prefix, int maxRatePerSec) {
            ArrayList<String> fileNameList = new ArrayList<String>();
            String[] names = dir.list();
            //Filter file name by partition prefix.
            for (String name : names) {
                if (name.startsWith(prefix) && name.endsWith(ReadOnlyUtils.INDEX_FILE_EXTENSION)) {
                    fileNameList.add(dir.getAbsolutePath() + "/" + name.substring(0, name.lastIndexOf('.')));
                }
            }
            //Sort by chunk number
            fileNameList.sort(new ReadOnlyFileNameComparator());
            fileNameIter = fileNameList.iterator();

            throttler = new EventThrottler(maxRatePerSec);
        }

        @Override
        public boolean hasNext() {
            try {
                while (indexInputStream == null || indexInputStream.available() <= 0) {
                    clearResource();
                    Arrays.fill(hash, (byte) 0);
                    Arrays.fill(position, (byte) 0);
                    if (fileNameIter.hasNext()) {
                        try {
                            String name = fileNameIter.next();
                            indexInputStream = new FileInputStream(name + ReadOnlyUtils.INDEX_FILE_EXTENSION);
                            dataInputStream = new FileInputStream(name + ReadOnlyUtils.DATA_FILE_EXTENSION);
                        } catch (FileNotFoundException e) {
                            clearResource();
                            throw new IllegalStateException("Can not find file.", e);
                        }
                    } else {
                        return false;
                    }
                }
            } catch (IOException e) {
                clearResource();
                throw new IllegalStateException("Met error when reading from index file.", e);
            }
            return true;
        }

        /**
         * Return the next hash value of records(If collision, there are many records in same hash value).
         */
        @Override
        public byte[] next() {
            if (hasNext()) {
                try {
                    int bytesRead = 0;
                    bytesRead += indexInputStream.read(hash);
                    bytesRead += indexInputStream.read(position);
                    throttler.maybeThrottle(bytesRead);
                    return hash;
                } catch (IOException e) {
                    throw new IllegalStateException("Met error when reading from index file.", e);
                }
            }
            return null;
        }

        /**
         * Read key-value pairs of current records.
         */
        protected Pair<byte[], byte[]>[] readRecords() {
            try {
                int bytesRead = 0;
                dataInputStream.getChannel().position((long) getInt(position));
                bytesRead += dataInputStream.read(numKeysBytes);
                int numKeys = (int) (ByteBuffer.wrap(numKeysBytes).getShort());
                @SuppressWarnings("unchecked")
                Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>(null, null);
                //Useless pair object which is only used to create generic array.
                Pair<byte[], byte[]>[] records = (Pair<byte[], byte[]>[]) Array.newInstance(pair.getClass(), numKeys);
                while (numKeys > 0) {
                    bytesRead += dataInputStream.read(keySizeBytes);
                    bytesRead += dataInputStream.read(valueSizeBytes);
                    byte[] key = new byte[getInt(keySizeBytes)];
                    byte[] value = new byte[getInt(valueSizeBytes)];
                    bytesRead += dataInputStream.read(key);
                    bytesRead += dataInputStream.read(value);
                    //Order here is not important.
                    records[numKeys - 1] = new Pair<byte[], byte[]>(key, value);
                    numKeys--;
                }
                throttler.maybeThrottle(bytesRead);
                return records;
            } catch (IOException e) {
                clearResource();
                throw new IllegalStateException("Met error when reading from data file.", e);
            }
        }

        public void clearResource() {
            close(indexInputStream);
            close(dataInputStream);
            indexInputStream = null;
            dataInputStream = null;
        }
    }

    private static <T extends Closeable> void close(T closeableObj) {
        if (closeableObj != null) {
            try {
                closeableObj.close();
            } catch (IOException e) {
                //ignore
            }
        }
    }

    private static int getInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    private static File getVersionDir(String path, String store, int version) {
        return new File(path + "/" + store + "/version-" + version);
    }

    private static HashSet<String> getPartitionPrefixes(File dir) {
        String[] files = dir.list();
        HashSet<String> partitions = new HashSet<String>();
        for (String file : files) {
            if (file.endsWith(ReadOnlyUtils.DATA_FILE_EXTENSION)) {
                partitions.add(file.substring(0, file.lastIndexOf('_')));
            }
        }
        return partitions;
    }
}
