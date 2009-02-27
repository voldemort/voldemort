package voldemort.contrib.batchswapper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.TextInputFormat;

public class NonSplitableDummyFileInputFormat extends TextInputFormat {

    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    class DummyRecordReader extends LineRecordReader {

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
