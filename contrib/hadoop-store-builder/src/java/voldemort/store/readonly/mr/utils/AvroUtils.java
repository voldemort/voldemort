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

package voldemort.store.readonly.mr.utils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AvroUtils {

    /**
     * Pull the schema off of the given file (if it is a file). If it is a
     * directory, then pull schemas off of all subfiles, and check that they are
     * all the same schema. If so, return that schema, otherwise throw an
     * exception
     * 
     * @param fs The filesystem to use
     * @param path The path from which to get the schema
     * @return The schema of this file or all its subfiles
     * @throws IOException
     */

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Schema getSchemaFromPath(FileSystem fs, Path path) {

        try {
            if(fs.isFile(path)) {
                BufferedInputStream inStream = null;
                try {
                    inStream = new BufferedInputStream(fs.open(path));
                } catch(IOException e1) {
                    throw new RuntimeException("Unable to open " + path, e1);
                }
                GenericDatumReader datum = new GenericDatumReader();

                DataFileStream reader = null;
                try {
                    reader = new DataFileStream(inStream, datum);
                } catch(IOException e) {
                    throw new RuntimeException("Invalid avro format, path " + path, e);
                }
                return reader.getSchema();
            } else {
                FileStatus[] statuses = null;
                if(fs.isDirectory(path)) {
                    // this is a directory, get schemas from all subfiles
                    statuses = fs.listStatus(path);
                    if(statuses == null || statuses.length == 0)
                        throw new IllegalArgumentException("No files in directory " + path);
                } else {
                    // this is wildcard path, get schemas from all matched files
                    statuses = fs.globStatus(path);
                    if(statuses == null || statuses.length == 0)
                        throw new IllegalArgumentException("No matches for path pattern " + path);
                }
                List<Schema> schemas = new ArrayList<Schema>();
                for(FileStatus status: statuses) {
                    if(!HadoopUtils.shouldPathBeIgnored(status.getPath())) {
                        schemas.add(getSchemaFromPath(fs, status.getPath()));
                    }
                }

                // now check that all the schemas are the same
                if(schemas.size() > 0) {
                    Schema schema = schemas.get(0);
                    for(int i = 1; i < schemas.size(); i++)
                        if(!schema.equals(schemas.get(i)))
                            throw new IllegalArgumentException("The directory "
                                                               + path
                                                               + " contains heterogeneous schemas: found both '"
                                                               + schema + "' and '"
                                                               + schemas.get(i) + "'.");

                    return schema;
                } else {
                    throw new IllegalArgumentException("No valid metadata file found for path " + path);
                }
            }
        } catch(Exception e) {
            throw new RuntimeException("Error getting schema for path " + path, e);
        }
    }

    public static Schema getAvroSchemaFromPath(Path path) throws IOException {
        return getSchemaFromPath(path.getFileSystem(new Configuration()), path);
    }

}
