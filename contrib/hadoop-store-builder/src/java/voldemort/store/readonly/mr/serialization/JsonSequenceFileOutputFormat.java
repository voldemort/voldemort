package voldemort.store.readonly.mr.serialization;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import voldemort.serialization.json.JsonTypeDefinition;

/**
 * Copy of hadoop's SequenceFileOutputFormat modified to set the schema as metadata on
 * output files
 * 
 * @author jkreps
 * 
 */
public class JsonSequenceFileOutputFormat extends
    SequenceFileOutputFormat<BytesWritable, BytesWritable>
{

  public RecordWriter<BytesWritable, BytesWritable> getRecordWriter(FileSystem ignored,
                                                                    JobConf job,
                                                                    String name,
                                                                    Progressable progress)
      throws IOException
  {

    // Shamelessly copy in hadoop code to allow us to set the metadata with our schema

    // get the path of the temporary output file
    Path file = FileOutputFormat.getTaskOutputPath(job, name);

    FileSystem fs = file.getFileSystem(job);
    CompressionType compressionType = CompressionType.BLOCK;
    // find the right codec
    Class<?> codecClass = getOutputCompressorClass(job, DefaultCodec.class);
    CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, job);

    // set the schema metadata
    /* begin jays code */
    SequenceFile.Metadata meta = new SequenceFile.Metadata();
    meta.set(new Text("key.schema"),
             new Text(getSchema("reducer.output.key.schema", job)));
    meta.set(new Text("value.schema"), new Text(getSchema("reducer.output.value.schema",
                                                          job)));

    final SequenceFile.Writer out = SequenceFile.createWriter(fs,
                                                              job,
                                                              file,
                                                              job.getOutputKeyClass(),
                                                              job.getOutputValueClass(),
                                                              compressionType,
                                                              codec,
                                                              progress,
                                                              meta);
    /* end jays code */

    return new RecordWriter<BytesWritable, BytesWritable>()
    {

      public void write(BytesWritable key, BytesWritable value)
          throws IOException
      {

        out.append(key, value);
      }

      public void close(Reporter reporter)
          throws IOException
      {
        out.close();
      }
    };
  }

  private String getSchema(String prop, JobConf conf)
  {
    String schema = conf.get(prop);
    if (schema == null)
      throw new IllegalArgumentException("The required property '" + prop
          + "' is not defined in the JobConf for this Hadoop job.");
    // check that it is a valid schema definition
    JsonTypeDefinition.fromJson(schema);

    return schema;
  }

}
