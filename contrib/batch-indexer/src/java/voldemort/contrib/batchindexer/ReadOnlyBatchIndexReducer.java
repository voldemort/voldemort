package voldemort.contrib.batchindexer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import voldemort.utils.ByteUtils;

public class ReadOnlyBatchIndexReducer implements Reducer<BytesWritable, BytesWritable, Text, Text> {

    private DataOutputStream _indexFileStream = null;
    private DataOutputStream _valueFileStream = null;

    private long _position = 0;

    private JobConf _conf = null;
    private String _taskId = null;
    private int _nodeId = -1;

    Path taskIndexFileName;
    Path taskValueFileName;

    /**
     * Reduce should get sorted MD5 keys here with a single value (appended in
     * begining with 4 bits of nodeId)
     */
    public void reduce(BytesWritable key,
                       Iterator<BytesWritable> values,
                       OutputCollector<Text, Text> output,
                       Reporter reporter) throws IOException {
        byte[] keyBytes = ByteUtils.copy(key.get(), 0, key.getSize());

        while(values.hasNext()) {
            BytesWritable value = values.next();
            byte[] valBytes = ByteUtils.copy(value.get(), 0, value.getSize());

            if(_nodeId == -1) {
                DataInputStream buffer = new DataInputStream(new ByteArrayInputStream(valBytes));
                _nodeId = buffer.readInt();
            }
            // strip first 4 bytes as node_id
            byte[] value1 = ByteUtils.copy(valBytes, 4, valBytes.length);

            // Write Index Key/ position
            _indexFileStream.write(keyBytes);
            _indexFileStream.writeLong(_position);
            _valueFileStream.writeInt(value1.length);
            _valueFileStream.write(value1);
            _position += value1.length + 4;

            if(_position < 0) {
                throw new RuntimeException("Position bigger than Integer size, split input files.");
            }
        }

    }

    public void configure(JobConf job) {
        try {
            _position = 0;
            _conf = job;

            _taskId = job.get("mapred.task.id");

            taskIndexFileName = new Path(FileOutputFormat.getOutputPath(_conf),
                                         _conf.get("voldemort.index.filename") + "_" + _taskId);
            taskValueFileName = new Path(FileOutputFormat.getOutputPath(_conf),
                                         _conf.get("voldemort.data.filename") + "_" + _taskId);

            FileSystem fs = taskIndexFileName.getFileSystem(job);

            _indexFileStream = fs.create(taskIndexFileName, (short) 1);
            _valueFileStream = fs.create(taskValueFileName, (short) 1);
        } catch(IOException e) {
            throw new RuntimeException("Failed to open Input/OutputStream", e);
        }
    }

    public void close() throws IOException {

        _indexFileStream.close();
        _valueFileStream.close();

        Path hdfsIndexFile = new Path(FileOutputFormat.getOutputPath(_conf), _nodeId + ".index");
        Path hdfsValueFile = new Path(FileOutputFormat.getOutputPath(_conf), _nodeId + ".data");

        FileSystem fs = hdfsIndexFile.getFileSystem(_conf);
        fs.rename(taskIndexFileName, hdfsIndexFile);
        fs.rename(taskValueFileName, hdfsValueFile);
    }
}
