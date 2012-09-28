/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.readonly.mr.serialization;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import voldemort.store.readonly.mr.utils.HadoopUtils;

/**
 * Extends {@link SequenceFileInputFormat} to support our JSON based
 * serialization format.
 * 
 * Reads in a SequenceFile Read out the schema from Metadata and save it as keys
 * in configuration.
 * <ul>
 * <li>"mapper.input.key.schema"</li>
 * <li>"mapper.input.value.schema"</li>
 * </ul>
 * 
 * @author bbansal
 * 
 */
public class JsonSequenceFileInputFormat extends
        SequenceFileInputFormat<BytesWritable, BytesWritable> {

    protected static final Logger log = Logger.getLogger(JsonSequenceFileInputFormat.class.getName());

    @Override
    public RecordReader<BytesWritable, BytesWritable> getRecordReader(InputSplit split,
                                                                      JobConf conf,
                                                                      Reporter reporter)
            throws IOException {
        String inputPathString = ((FileSplit) split).getPath().toUri().getPath();
        log.info("Input file path:" + inputPathString);
        Path inputPath = new Path(inputPathString);

        SequenceFile.Reader reader = new SequenceFile.Reader(inputPath.getFileSystem(conf),
                                                             inputPath,
                                                             conf);
        SequenceFile.Metadata meta = reader.getMetadata();

        try {
            Text keySchema = meta.get(new Text("key.schema"));
            Text valueSchema = meta.get(new Text("value.schema"));

            if(0 == keySchema.getLength() || 0 == valueSchema.getLength()) {
                throw new Exception();
            }

            // update Joboconf with schemas
            conf.set("mapper.input.key.schema", keySchema.toString());
            conf.set("mapper.input.value.schema", valueSchema.toString());
        } catch(Exception e) {
            throw new IOException("Failed to Load Schema from file:" + inputPathString + "\n");
        }
        return super.getRecordReader(split, conf, reporter);
    }

    @Override
    protected FileStatus[] listStatus(JobConf job) throws IOException {
        String dirs = job.get("mapred.input.dir", "");
        String[] list = StringUtils.split(dirs);

        List<FileStatus> status = new ArrayList<FileStatus>();
        for(int i = 0; i < list.length; i++) {
            status.addAll(getAllSubFileStatus(job, new Path(list[i])));
        }

        return status.toArray(new FileStatus[0]);
    }

    private List<FileStatus> getAllSubFileStatus(JobConf inputConf, Path filterMemberPath)
            throws IOException {
        List<FileStatus> list = new ArrayList<FileStatus>();

        FileSystem fs = filterMemberPath.getFileSystem(inputConf);
        FileStatus[] subFiles = fs.listStatus(filterMemberPath);

        if(null != subFiles) {
            if(fs.isDirectory(filterMemberPath)) {
                for(FileStatus subFile: subFiles) {
                    if(!HadoopUtils.shouldPathBeIgnored(subFile.getPath())) {
                        list.addAll(getAllSubFileStatus(inputConf, subFile.getPath()));
                    }
                }
            } else {
                if(subFiles.length > 0 && !HadoopUtils.shouldPathBeIgnored(subFiles[0].getPath())) {
                    list.add(subFiles[0]);
                }
            }
        }

        return list;
    }
}
