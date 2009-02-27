package voldemort.contrib.batchswapper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

public class NonSplitableDummyFileInputFormat extends TextInputFormat {

    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit,
                                                            JobConf job,
                                                            Reporter reporter) throws IOException {

        reporter.setStatus(genericSplit.toString());
        return new DummyRecordReader(job, (FileSplit) genericSplit);
    }

    class DummyRecordReader extends LineRecordReader {

        private int counter = 0;

        public DummyRecordReader(Configuration conf, FileSplit split) throws IOException {
            super(conf, split);
        }

        @Override
        public boolean next(LongWritable key, Text value) throws IOException {
            if(counter >= 1) {
                return false;
            }
            counter++;
            return true;
        }
    }

}
