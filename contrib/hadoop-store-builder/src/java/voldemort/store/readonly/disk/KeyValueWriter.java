package voldemort.store.readonly.disk;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

// Interface used by reducers to layout the datqa on disk
public interface KeyValueWriter<K, V> {

    public static enum CollisionCounter {

        NUM_COLLISIONS,
        MAX_COLLISIONS;
    }

    public void conf(JobConf job);

    public void write(K key, Iterator<V> iterator, Reporter reporter) throws IOException;

    public void close() throws IOException;

}
