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
     * @param checkSameSchema boolean flag to check all files in directory for
     *        same schema
     * @return The schema of this file or all its subfiles
     * @throws IOException
     */

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Schema getSchemaFromPath(FileSystem fs, Path path, boolean checkSameSchema) {

        try {
            if(fs.isFile(path)) {
                BufferedInputStream inStream = null;
                try {
                    inStream = new BufferedInputStream(fs.open(path));
                } catch(IOException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                GenericDatumReader datum = new GenericDatumReader();

                DataFileStream reader = null;
                try {
                    reader = new DataFileStream(inStream, datum);
                } catch(IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                return reader.getSchema();
            } else {
                FileStatus[] statuses = null;
                if(fs.isDirectory(path)) {
                    // this is a directory, get schemas from all subfiles
                    statuses = fs.listStatus(path);
                } else {
                    // this is wildcard path, get schemas from all matched files
                    statuses = fs.globStatus(path);
                }
                if(statuses == null || statuses.length == 0)
                    throw new IllegalArgumentException("No files found in path pattern "
                                                       + path.toUri().getPath());
                List<Schema> schemas = new ArrayList<Schema>();
                for(FileStatus status: statuses) {
                    if(!HadoopUtils.shouldPathBeIgnored(status.getPath())) {
                        if(!checkSameSchema) {
                            // return first valid schema w/o checking all files
                            return getSchemaFromPath(fs, status.getPath(), checkSameSchema);
                        }
                        schemas.add(getSchemaFromPath(fs, status.getPath(), checkSameSchema));
                    }
                }

                // now check that all the schemas are the same
                if(schemas.size() > 0) {
                    Schema schema = schemas.get(0);
                    for(int i = 1; i < schemas.size(); i++)
                        if(!schema.equals(schemas.get(i)))
                            throw new IllegalArgumentException("The directory "
                                                               + path.toString()
                                                               + " contains heterogenous schemas: found both '"
                                                               + schema.toString() + "' and '"
                                                               + schemas.get(i).toString() + "'.");

                    return schema;
                } else {
                    throw new IllegalArgumentException("No Valid metadata file found for Path:"
                                                       + path.toString());
                }
            }
        } catch(Exception e) {
            // logger.error("failed to get metadata from path:" + path);
            throw new RuntimeException(e);
        }

    }

    public static Schema getAvroSchemaFromPath(Path path) throws IOException {
        return getSchemaFromPath(path.getFileSystem(new Configuration()), path, true);
    }

}
