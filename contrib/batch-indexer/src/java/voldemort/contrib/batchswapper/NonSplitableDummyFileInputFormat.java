package voldemort.contrib.batchswapper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;

public class NonSplitableDummyFileInputFormat extends
        SequenceFileInputFormat<BytesWritable, BytesWritable> {

    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    class DummyRecordReader extends SequenceFileRecordReader<BytesWritable, BytesWritable> {

        private int counter = 0;

        public DummyRecordReader(Configuration conf, FileSplit split) throws IOException {
            super(conf, split);
        }

        public boolean next(BytesWritable key, BytesWritable value) throws IOException {
            if(counter >= 1) {
                return false;
            }
            counter++;
            return true;
        }
    }

}
