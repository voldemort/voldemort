package voldemort.contrib.batchindexer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

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

    String indexFileName;
    String dataFileName;
    String taskIndexFileName;
    String taskValueFileName;

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

            indexFileName = new Path(_conf.get("voldemort.index.filename")).getName();
            taskIndexFileName = _conf.getLocalPath(indexFileName + "_" + _taskId).toUri().getPath();

            dataFileName = new Path(_conf.get("voldemort.data.filename")).getName();
            taskValueFileName = _conf.getLocalPath(dataFileName + "_" + _taskId).toUri().getPath();

            _indexFileStream = new DataOutputStream(new java.io.BufferedOutputStream(new FileOutputStream(new File(taskIndexFileName))));
            _valueFileStream = new DataOutputStream(new java.io.BufferedOutputStream(new FileOutputStream(new File(taskValueFileName))));
        } catch(IOException e) {
            new RuntimeException("Failed to open Input/OutputStream", e);
        }
    }

    public void close() throws IOException {
        // close the local file stream and copy it to HDFS
        _indexFileStream.close();
        _valueFileStream.close();

        Path hdfsIndexFile = new Path(FileOutputFormat.getOutputPath(_conf), indexFileName + "_"
                                                                             + _nodeId);
        Path hdfsValueFile = new Path(FileOutputFormat.getOutputPath(_conf), dataFileName + "_"
                                                                             + _nodeId);

        hdfsIndexFile.getFileSystem(_conf).copyFromLocalFile(new Path(taskIndexFileName),
                                                             hdfsIndexFile);
        hdfsValueFile.getFileSystem(_conf).copyFromLocalFile(new Path(taskValueFileName),
                                                             hdfsValueFile);
    }
}
