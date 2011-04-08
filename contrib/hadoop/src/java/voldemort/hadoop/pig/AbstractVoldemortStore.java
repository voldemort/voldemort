package voldemort.hadoop.pig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import voldemort.hadoop.VoldemortHadoopConfig;
import voldemort.hadoop.VoldemortInputFormat;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import java.io.IOException;

/**
 * Superclass for Voldemort Pig Stores. The Tuple format is specified by subclasses.
 */
public abstract class AbstractVoldemortStore extends LoadFunc {
    protected RecordReader reader;

    @Override
    public InputFormat getInputFormat() throws IOException {
        VoldemortInputFormat inputFormat = new VoldemortInputFormat();
        return inputFormat;
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.reader = reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        if(!location.startsWith("tcp://"))
            throw new IOException("The correct format is tcp://<url:port>/storeName");
        String[] subParts = location.split("/+");
        Configuration conf = job.getConfiguration();
        VoldemortHadoopConfig.setVoldemortURL(conf, subParts[0] + "//" + subParts[1]);
        VoldemortHadoopConfig.setVoldemortStoreName(conf, subParts[2]);
    }

    @Override
    public Tuple getNext() throws IOException {
        ByteArray key = null;
        Versioned<byte[]> value = null;

        try {

            if(!reader.nextKeyValue())
                return null;
            key = (ByteArray) reader.getCurrentKey();
            value = (Versioned<byte[]>) reader.getCurrentValue();

        } catch(InterruptedException e) {
            throw new IOException("Error reading in key/value");
        }

        if(key == null || value == null) {
            return null;
        }

        return extractTuple(key, value);
    }

    protected abstract Tuple extractTuple(ByteArray key, Versioned<byte[]> value) throws ExecException;
}
