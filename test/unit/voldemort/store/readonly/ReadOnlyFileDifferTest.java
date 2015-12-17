package voldemort.store.readonly;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import junit.framework.TestCase;
import voldemort.utils.Pair;


/**
 * Unit tests for ReadOnlyFileDiffer.
 */
public class ReadOnlyFileDifferTest extends TestCase {

    public void testByteArrayComparator() {
        ReadOnlyFileDiffer.ByteArrayComparator comparator = new ReadOnlyFileDiffer.ByteArrayComparator();
        //Equal byte array.
        byte[] b1 = new byte[]{1, 2, 3};
        byte[] b2 = new byte[]{1, 2, 3};
        assertEquals(0, comparator.compare(b1, b2));
        //b2 is longer than b1, so b2>b1
        b1 = new byte[]{1, 2, 3};
        b2 = new byte[]{1, 2, 3, 4};
        assertTrue(comparator.compare(b1, b2) < 0);
        //b1 is longer than b2, so b1>b2
        b1 = new byte[]{1, 2, 3};
        b2 = new byte[]{1, 2};
        assertTrue(comparator.compare(b1, b2) > 0);

        b1 = new byte[]{1, 3, 2};
        b2 = new byte[]{1, 2, 2};
        assertTrue(comparator.compare(b1, b2) > 0);

        b1 = new byte[]{1, 2, 2};
        b2 = new byte[]{1, 2, 3};
        assertTrue(comparator.compare(b1, b2) < 0);

        b1 = new byte[]{5};
        b2 = new byte[]{1, 2, 3};
        assertTrue(comparator.compare(b1, b2) > 0);
    }

    public void testReadOnlyFileNameComparator() {
        ReadOnlyFileDiffer.ReadOnlyFileNameComparator comparator = new ReadOnlyFileDiffer.ReadOnlyFileNameComparator();

        String s1 = "123_1_0";
        String s2 = "234_0_0";
        assertEquals(0, comparator.compare(s1, s2));

        s1 = "123_1_0";
        s2 = "234_0_1";
        assertTrue(comparator.compare(s1, s2) < 0);

        s1 = "123_1_10";
        s2 = "234_0_0";
        assertTrue(comparator.compare(s1, s2) > 0);

        s1 = "123";
        s2 = "234_0_1";
        try {
            comparator.compare(s1, s2);
            fail("Should met exception when compare invalid file name");
        } catch (Throwable e) {
            //Expected
        }
    }

    public void testIterateRecords()
        throws IOException {
        String fName1 = "123_0";
        String fName2 = "234_1";
        ArrayList<File> files = new ArrayList<File>();
        try {
            files.addAll(createTestFile(fName1, 0, 0, 10));
            files.addAll(createTestFile(fName1, 1, 0, 0));
            files.addAll(createTestFile(fName1, 2, 0, 5));
            files.addAll(createTestFile(fName2, 0, 0, 5));

            ReadOnlyFileDiffer.ReadOnlyRecordIterator iter =
                new ReadOnlyFileDiffer.ReadOnlyRecordIterator(new File("./"), fName1);
            int i = 0;
            byte[] hash = null;
            while (i < 11) {
                if (!iter.hasNext()) {
                    fail("Iterator error, should have next record.");
                }
                hash = iter.next();
                System.out.println(ByteBuffer.wrap(hash).getLong());
                i++;
            }
            //chunk 2 is empty, now the element should be the first element of chunk 3
            assertEquals(0l, ByteBuffer.wrap(hash).getLong());
            Pair<byte[], byte[]>[] pair = iter.readRecords();
            assertEquals(1, pair.length);
            assertEquals(0l, ByteBuffer.wrap(pair[0].getFirst()).getLong());
            ByteBuffer value = ByteBuffer.wrap(pair[0].getSecond());
            assertEquals(0l, value.getLong());
            assertEquals(1l, value.getLong());

            while (iter.hasNext()) {
                iter.next();
                i++;
            }
            assertEquals(15, i);
        } finally {
            cleanTestFiles(files);
        }
    }

    public void testCompare()
        throws IOException {
        ReadOnlyFileDiffer.ByteArrayComparator comparator = new ReadOnlyFileDiffer.ByteArrayComparator();
        ArrayList<File> files = new ArrayList<File>();
        File version1 = new File("version1");
        File version2 = new File("version2");
        try {
            version1.mkdir();
            version2.mkdir();
            //Same key-value pairs, but they are located in different chunk for different versions.
            String prefix = "123_0";
            files.addAll(createTestFile("version1/" + prefix, 0, 0, 10));
            files.addAll(createTestFile("version1/" + prefix, 1, 0, 0));
            files.addAll(createTestFile("version1/" + prefix, 2, 10, 5));

            files.addAll(createTestFile("version2/" + prefix, 0, 0, 5));
            files.addAll(createTestFile("version2/" + prefix, 1, 5, 5));
            files.addAll(createTestFile("version2/" + prefix, 2, 10, 5));

            ReadOnlyFileDiffer.ReadOnlyRecordIterator oldIter =
                new ReadOnlyFileDiffer.ReadOnlyRecordIterator(new File("version1"), prefix);
            ReadOnlyFileDiffer.ReadOnlyRecordIterator newIter =
                new ReadOnlyFileDiffer.ReadOnlyRecordIterator(new File("version2"), prefix);

            ReadOnlyFileDiffer analyzer = new ReadOnlyFileDiffer();
            long[] results = analyzer.comparePartition(comparator, oldIter, newIter);
            assertEquals(0, results[0]);
            assertEquals(0, results[1]);
            assertEquals(0, results[2]);
            assertEquals(15, results[3]);

            //Version2 delete key6 and key9, add key5 and key 15
            prefix = "234_0";
            files.addAll(createTestFile("version1/" + prefix, 0, 0, 5));
            files.addAll(createTestFile("version1/" + prefix, 1, 6, 5));
            files.addAll(createTestFile("version1/" + prefix, 2, 11, 4));

            files.addAll(createTestFile("version2/" + prefix, 0, 0, 6));
            files.addAll(createTestFile("version2/" + prefix, 1, 7, 2));
            files.addAll(createTestFile("version2/" + prefix, 3, 10, 6));

            oldIter = new ReadOnlyFileDiffer.ReadOnlyRecordIterator(new File("version1"), prefix);
            newIter = new ReadOnlyFileDiffer.ReadOnlyRecordIterator(new File("version2"), prefix);

            analyzer = new ReadOnlyFileDiffer();
            results = analyzer.comparePartition(comparator, oldIter, newIter);
            assertEquals(2, results[0]);
            assertEquals(2, results[1]);
            assertEquals(0, results[2]);
            assertEquals(12, results[3]);

            //Version2 add key7,key10000 , delete key3, key12, update key3
            prefix = "345_0";
            files.addAll(createTestFile("version1/" + prefix, 0, 0, 7));
            files.addAll(createTestFile("version1/" + prefix, 1, 8, 5));

            files.addAll(createTestFile("version2/" + prefix, 0, 0, 12));
            RandomAccessFile dataFile =
                new RandomAccessFile("version2/" + prefix + "_" + 0 + ReadOnlyUtils.DATA_FILE_EXTENSION, "rw");
            //Change the value of key2
            dataFile.seek(34 * 2 + 18);
            dataFile.writeLong(1000l);
            //Change key3 to key10000
            dataFile.skipBytes(8 + 10);
            dataFile.writeLong(10000l);
            dataFile.getFD().sync();
            dataFile.close();
            oldIter = new ReadOnlyFileDiffer.ReadOnlyRecordIterator(new File("version1"), prefix);
            newIter = new ReadOnlyFileDiffer.ReadOnlyRecordIterator(new File("version2"), prefix);

            analyzer = new ReadOnlyFileDiffer();
            results = analyzer.comparePartition(comparator, oldIter, newIter);
            assertEquals(2, results[0]);
            assertEquals(2, results[1]);
            assertEquals(1, results[2]);
            assertEquals(9, results[3]);
        } finally {
            files.add(version1);
            files.add(version2);
            cleanTestFiles(files);
        }
    }

    private Collection<File> createTestFile(String prefix, int chunk, int start, int numRecord)
        throws IOException {
        File index = new File(prefix + "_" + chunk + ReadOnlyUtils.INDEX_FILE_EXTENSION);
        File data = new File(prefix + "_" + chunk + ReadOnlyUtils.DATA_FILE_EXTENSION);

        if (index.exists()) {
            index.delete();
        }
        if (data.exists()) {
            data.delete();
        }

        DataOutputStream indexOutputStream = null;
        DataOutputStream dataOutputStream = null;
        try {
            indexOutputStream = new DataOutputStream(new FileOutputStream(index));
            dataOutputStream = new DataOutputStream(new FileOutputStream(data));
            for (int i = start; i < start + numRecord; i++) {
                indexOutputStream.writeLong(i);
                indexOutputStream.writeInt(dataOutputStream.size());
                dataOutputStream.writeShort(1);
                dataOutputStream.writeInt(8);
                dataOutputStream.writeInt(16);
                dataOutputStream.writeLong(i);
                dataOutputStream.writeLong(i);
                dataOutputStream.writeLong(i + 1);
            }
            indexOutputStream.flush();
            dataOutputStream.flush();
            return Arrays.asList(new File[]{index, data});
        } finally {
            if (indexOutputStream != null) {
                indexOutputStream.close();
            }
            if (dataOutputStream != null) {
                dataOutputStream.close();
            }
        }
    }

    private void cleanTestFiles(Collection<File> files) {
        for (File file : files) {
            file.delete();
        }
    }
}
