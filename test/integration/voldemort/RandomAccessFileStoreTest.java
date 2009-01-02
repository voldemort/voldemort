package voldemort;

import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import voldemort.store.readonly.RandomAccessFileStore;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableList;

/**
 * @author jay
 * 
 */
public class RandomAccessFileStoreTest {

    public static void main(String[] args) throws Exception {
        if(args.length != 2)
            Utils.croak("USAGE: java MmapStoreTest index-file data-file");

        BlockingQueue<RandomAccessFile> index = new ArrayBlockingQueue<RandomAccessFile>(1);
        index.add(new RandomAccessFile(args[0], "r"));

        BlockingQueue<RandomAccessFile> data = new ArrayBlockingQueue<RandomAccessFile>(1);
        data.add(new RandomAccessFile(args[1], "r"));

        RandomAccessFileStore store = null; // new RandomAccessFileStore("test",
        // 10000L, index, data);
        for(int i : ImmutableList.of(964, 2, 15, 78, 192, 984)) {
            byte[] key = Integer.toString(i).getBytes();
            List<Versioned<byte[]>> values = store.get(key);
            System.out.print(new String(key));
            System.out.print(' ');
            System.out.println(new String(values.get(0).getValue()));
        }
    }

}
