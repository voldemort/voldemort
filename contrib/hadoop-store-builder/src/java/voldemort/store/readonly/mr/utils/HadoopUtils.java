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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.joda.time.Period;

import voldemort.cluster.Cluster;
import voldemort.serialization.json.JsonTypeDefinition;
import voldemort.serialization.json.JsonTypes;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteUtils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;
import azkaban.common.utils.Props;
import azkaban.common.utils.UndefinedPropertyException;

/**
 * Helper functions for Hadoop
 * 
 * @author jkreps
 * 
 */
public class HadoopUtils {

    // Any date written with the pattern should be accepted by the regex.
    public static String COMMON_FILE_DATE_PATTERN = "yyyy-MM-dd-HH-mm";
    public static String COMMON_FILE_DATE_REGEX = "\\d{4}-\\d{2}-\\d{2}-\\d{2}-\\d{2}";

    private static Logger logger = Logger.getLogger(HadoopUtils.class);
    private static Object cachedSerializable = null;

    public static FileSystem getFileSystem(Props props) {
        if(!props.containsKey("hadoop.job.ugi"))
            throw new RuntimeException("No parameter hadoop.job.ugi set!");
        return getFileSystem(props.getString("hadoop.job.ugi"));
    }

    public static FileSystem getFileSystem(String user) {
        Configuration conf = new Configuration();
        conf.set("hadoop.job.ugi", user);
        try {
            return FileSystem.get(conf);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Add the given object to the distributed cache for this job
     * 
     * @param obj A Serializable object to add to the JobConf
     * @param job The JobConf
     */
    public static <T extends Serializable> void setSerializableInCache(JobConf job, T serializable) {
        try {
            // TODO: MED /tmp should be changed by conf.getTempDir() or
            // something
            Path workDir = new Path(String.format("/tmp/%s/%s/_join.temporary",
                                                  job.getJobName(),
                                                  System.currentTimeMillis()));

            Path tempPath = new Path(workDir, "serializable.dat");
            tempPath.getFileSystem(job).deleteOnExit(tempPath);
            job.set("serializables.file", tempPath.toUri().getPath());

            ObjectOutputStream objectStream = new ObjectOutputStream(tempPath.getFileSystem(job)
                                                                             .create(tempPath));
            objectStream.writeObject(serializable);
            objectStream.close();

            DistributedCache.addCacheFile(new URI(tempPath.toUri().getPath() + "#"
                                                  + tempPath.getName()),
                                          job);
        } catch(URISyntaxException e) {
            throw new RuntimeException(e);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Path getFilePathFromDistributedCache(String filename, Configuration conf)
            throws IOException {

        Path[] paths = DistributedCache.getLocalCacheFiles(conf);
        Path filePath = null;
        File file = new File(filename);

        if(paths == null) {
            // maybe we are in local mode and hadoop is a complete piece of
            // shit that doesn't
            // work in local mode
            // check if maybe the file is just sitting there on the
            // filesystem
            if(file.exists())
                filePath = new Path(file.getAbsolutePath());
        } else {
            for(Path path: paths)
                if(path.getName().equals(file.getName()))
                    filePath = path;
        }

        return filePath;

    }

    /**
     * Get the FileInputStream from distributed cache
     * 
     * @param conf the JobConf
     * @return FileInputStream file input stream
     * @throws IOException
     */
    public static FileInputStream getFileInputStream(String filename, Configuration conf) {
        try {
            Path filePath = getFilePathFromDistributedCache(filename, conf);

            if(filePath == null) {
                Path[] paths = DistributedCache.getLocalCacheFiles(conf);
                throw new IllegalStateException("No cache file found by the name of '" + filename
                                                + "', found only " + paths);
            }
            return new FileInputStream(filePath.toString());
        } catch(IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Get the given Serializable from the distributed cache as an Object
     * 
     * @param conf The JobConf
     * @return The Object that is read from cache
     */
    public static Object readSerializableFromCache(Configuration conf) {
        /*
         * Cache the results of this operation, as this function may be called
         * more than once by the same process (i.e., by combiners).
         */
        if(HadoopUtils.cachedSerializable != null)
            return HadoopUtils.cachedSerializable;

        try {
            String filename = conf.get("serializables.file");
            if(filename == null)
                return null;

            Path serializable = getFilePathFromDistributedCache(filename, conf);

            if(serializable == null) {
                Path[] paths = DistributedCache.getLocalCacheFiles(conf);
                throw new IllegalStateException("No serializable cache file found by the name of '"
                                                + filename + "', found only " + paths);
            }
            ObjectInputStream stream = new ObjectInputStream(new FileInputStream(serializable.toString()));
            Object obj = stream.readObject();
            stream.close();
            HadoopUtils.cachedSerializable = obj;
            return obj;
        } catch(IOException e) {
            throw new RuntimeException(e);
        } catch(ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, String> getMetadataFromSequenceFile(String fileName) {
        Path path = new Path(fileName);
        try {
            return getMetadataFromSequenceFile(path.getFileSystem(new Configuration()), path);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, String> getMetadataFromSequenceFile(FileSystem fs, String fileName) {
        return getMetadataFromSequenceFile(fs, new Path(fileName));
    }

    /**
     * Read the metadata from a hadoop SequenceFile
     * 
     * @param fs The filesystem to read from
     * @param fileName The file to read from
     * @return The metadata from this file
     */
    public static Map<String, String> getMetadataFromSequenceFile(FileSystem fs, Path path) {
        try {
            Configuration conf = new Configuration();
            conf.setInt("io.file.buffer.size", 4096);
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, new Configuration());
            SequenceFile.Metadata meta = reader.getMetadata();
            reader.close();
            TreeMap<Text, Text> map = meta.getMetadata();
            Map<String, String> values = new HashMap<String, String>();
            for(Map.Entry<Text, Text> entry: map.entrySet())
                values.put(entry.getKey().toString(), entry.getValue().toString());

            return values;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static JsonSchema getSchemaFromPath(Path path) throws IOException {
        return getSchemaFromPath(path.getFileSystem(new Configuration()), path, true);
    }

    public static JsonSchema getSchemaFromPath(FileSystem fs, Path path) throws IOException {
        return getSchemaFromPath(fs, path, true);
    }

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
    public static JsonSchema getSchemaFromPath(FileSystem fs, Path path, boolean checkSameSchema)
            throws IOException {
        try {
            if(fs.isFile(path)) {
                // this is a normal file, get a schema from it
                Map<String, String> m = HadoopUtils.getMetadataFromSequenceFile(fs, path);
                if(!m.containsKey("value.schema") || !m.containsKey("key.schema"))
                    throw new IllegalArgumentException("No schema found on file " + path.toString());
                return new JsonSchema(JsonTypeDefinition.fromJson(m.get("key.schema")),
                                      JsonTypeDefinition.fromJson(m.get("value.schema")));
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
                List<JsonSchema> schemas = new ArrayList<JsonSchema>();
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
                    JsonSchema schema = schemas.get(0);
                    for(int i = 1; i < schemas.size(); i++)
                        if(!schema.equals(schemas.get(i)))
                            throw new IllegalArgumentException("The directory "
                                                               + path.toString()
                                                               + " contains heterogenous schemas: found both '"
                                                               + schema.toString() + "' and '"
                                                               + schemas.get(i).toString() + "'.");

                    return schema;
                } else {
                    throw new IllegalArgumentException("No Valid metedata file found for Path:"
                                                       + path.toString());
                }
            }
        } catch(Exception e) {
            logger.error("failed to get metadata from path:" + path);
            throw new RuntimeException(e);
        }
    }

    public static String getRequiredString(Configuration conf, String name) {
        String val = conf.get(name);
        if(val == null)
            throw new IllegalArgumentException("Missing required parameter '" + name + "'.");
        else
            return val;
    }

    public static int getRequiredInt(Configuration conf, String name) {
        return Integer.parseInt(getRequiredString(conf, name));
    }

    public static void copyInProps(Props props, Configuration conf, String... keys) {
        for(String key: keys)
            if(props.get(key) != null)
                conf.set(key, props.get(key));
    }

    public static void copyInRequiredProps(Props props, Configuration conf, String... keys) {
        for(String key: keys)
            conf.set(key, props.getString(key));
    }

    /**
     * Add all the properties in the Props to the given Configuration
     * 
     * @param conf The Configuration
     * @param props The Props
     * @return The Configuration with all the new properties
     */
    public static void copyInAllProps(Props props, Configuration conf) {
        for(String key: props.keySet())
            conf.set(key, props.get(key));
    }

    public static void copyInLocalProps(Props props, Configuration conf) {
        for(String key: props.localKeySet())
            conf.set(key, props.get(key));
    }

    public static Props loadHadoopProps(Props parent, File hadoopConfDir) {
        // load hadoop properties
        Configuration config = new Configuration();

        config.addResource(new Path(new File(hadoopConfDir, "hadoop-default.xml").getAbsolutePath()));
        config.addResource(new Path(new File(hadoopConfDir, "hadoop-site.xml").getAbsolutePath()));

        // copy to props
        Props props = new Props(parent);
        for(Entry<String, String> entry: config)
            props.put(entry.getKey(), config.get(entry.getKey()));

        return props;
    }

    public static void setPropsInJob(Configuration conf, Props props) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            props.storeFlattened(output);
            conf.set("azkaban.props", new String(output.toByteArray(), "UTF-8"));
        } catch(IOException e) {
            throw new RuntimeException("This is not possible!", e);
        }
    }

    public static Props getPropsFromJob(Configuration conf) {
        String propsString = conf.get("azkaban.props");
        if(propsString == null)
            throw new UndefinedPropertyException("The required property azkaban.props was not found in the Configuration.");
        try {
            ByteArrayInputStream input = new ByteArrayInputStream(propsString.getBytes("UTF-8"));
            Properties properties = new Properties();
            properties.load(input);
            return new Props(null, properties);
        } catch(IOException e) {
            throw new RuntimeException("This is not possible!", e);
        }
    }

    public static Cluster readCluster(String clusterFile, Configuration conf) throws IOException {
        return new ClusterMapper().readCluster(new StringReader(readAsString(new Path(clusterFile))));
    }

    public static StoreDefinition readStoreDef(String storeFile,
                                               String storeName,
                                               Configuration conf) throws IOException {

        List<StoreDefinition> stores = new StoreDefinitionsMapper().readStoreList(new StringReader(readAsString(new Path(storeFile))));
        for(StoreDefinition def: stores) {
            if(def.getName().equals(storeName))
                return def;
        }
        throw new RuntimeException("Can't find store definition for store '" + storeName + "'.");
    }

    public static String getFileFromCache(Configuration conf, String fileName) throws IOException {
        if("local".equals(conf.get("mapred.job.tracker"))) {
            // For local mode Distributed cache is not set.
            // try getting the raw file path.
            URI[] uris = DistributedCache.getCacheFiles(conf);
            return getFileFromURIList(uris, fileName);
        } else {
            // For Distributed filesystem.
            Path[] pathList = DistributedCache.getLocalCacheFiles(conf);
            return getFileFromPathList(pathList, fileName);
        }
    }

    public static String getFileFromURIList(URI[] uris, String fileName) throws IOException {
        for(URI uri: uris) {
            if(uri.getPath().endsWith(fileName)) {
                // uri matched
                return uri.getPath();
            }
        }
        return null;
    }

    public static String getFileFromPathList(Path[] pathList, String fileName) {
        for(Path file: pathList) {
            logger.info("getUriWithFragment path:" + file.toUri().getPath() + " fileName:"
                        + fileName);
            if(file.getName().equals(fileName)) {
                logger.info("FOUND getUriWithFragment path:" + file.toUri().getPath());
                return file.toUri().getPath();
            }
        }

        return null;
    }

    /**
     * Find a jar that contains a class of the same name, if any. It will return
     * a jar file, even if that is not the first thing on the class path that
     * has a class with the same name.
     * 
     * @param my_class the class to find.
     * @return a jar file that contains the class, or null.
     * @throws IOException
     */
    public static String findContainingJar(Class my_class, ClassLoader loader) {
        String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
        return findContainingJar(class_file, loader);
    }

    public static List<String> getFileNames(FileStatus[] statuses) {
        List<String> fileNames = new ArrayList<String>();
        if(statuses == null)
            return fileNames;
        for(FileStatus status: statuses)
            fileNames.add(status.getPath().getName());
        return fileNames;
    }

    public static String findContainingJar(String fileName, ClassLoader loader) {
        try {
            for(Enumeration itr = loader.getResources(fileName); itr.hasMoreElements();) {
                URL url = (URL) itr.nextElement();
                logger.info("findContainingJar finds url:" + url);
                if("jar".equals(url.getProtocol())) {
                    String toReturn = url.getPath();
                    if(toReturn.startsWith("file:")) {
                        toReturn = toReturn.substring("file:".length());
                    }
                    toReturn = URLDecoder.decode(toReturn, "UTF-8");
                    return toReturn.replaceAll("!.*$", "");
                }
            }
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static String printAllClassLoaderPaths(String fileName, ClassLoader loader) {
        try {
            for(Enumeration itr = loader.getResources(fileName); itr.hasMoreElements();) {
                URL url = (URL) itr.nextElement();
                logger.info("findContainingJar finds url:" + url);
            }
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static Period parsePeriod(String periodStr) {
        Matcher monthsFormat = Pattern.compile("[0-9][0-9]*M").matcher(periodStr);
        Matcher daysFormat = Pattern.compile("[0-9][0-9]*d").matcher(periodStr);
        Matcher hoursFormat = Pattern.compile("[0-9][0-9]*h").matcher(periodStr);
        Matcher minutesFormat = Pattern.compile("[0-9][0-9]*m").matcher(periodStr);

        Period period = new Period();
        while(monthsFormat.find()) {
            period = period.plusMonths(Integer.parseInt(monthsFormat.group()
                                                                    .substring(0,
                                                                               monthsFormat.group()
                                                                                           .length() - 1)));
        }
        while(daysFormat.find()) {
            period = period.plusDays(Integer.parseInt(daysFormat.group()
                                                                .substring(0,
                                                                           daysFormat.group()
                                                                                     .length() - 1)));
        }
        while(hoursFormat.find()) {
            period = period.plusHours(Integer.parseInt(hoursFormat.group()
                                                                  .substring(0,
                                                                             hoursFormat.group()
                                                                                        .length() - 1)));
        }
        while(minutesFormat.find()) {
            period = period.plusMinutes(Integer.parseInt(minutesFormat.group()
                                                                      .substring(0,
                                                                                 minutesFormat.group()
                                                                                              .length() - 1)));
        }

        return period;
    }

    public static FileSystem getFileSystem(String hdfsUrl, boolean isLocal) throws IOException {
        // Initialize fs
        FileSystem fs;
        if(isLocal) {
            fs = FileSystem.getLocal(new Configuration());
        } else {
            fs = new DistributedFileSystem();
            try {
                fs.initialize(new URI(hdfsUrl), new Configuration());
            } catch(URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        }
        return fs;
    }

    /**
     * Given a directory path, return the paths of all directories in that tree
     * that have no sub-directories.
     * 
     * @param directory
     * @return
     */
    public static List<String> getLowestLevelDirectories(FileSystem fs,
                                                         Path directory,
                                                         PathFilter pathFilter) throws IOException {
        List<String> lowestLevelDirectories = new ArrayList<String>();

        if(hasSubDirectories(fs, directory)) {
            // recurse on each of this directory's sub-directories, ignoring any
            // files in the
            // directory
            FileStatus[] statuses = fs.listStatus(directory);
            for(FileStatus status: statuses) {
                if(status.isDir()) {
                    lowestLevelDirectories.addAll(getLowestLevelDirectories(fs,
                                                                            status.getPath(),
                                                                            pathFilter));
                }
            }
        } else if(pathFilter == null || pathFilter.accept(directory)) {
            // this directory has no sub-directories, and either there is no
            // filter or it passes the
            // filter, so add it and return
            lowestLevelDirectories.add(directory.toString());
        }

        return lowestLevelDirectories;
    }

    /**
     * Given a string representation of a directory path, check whether or not
     * the directory has any sub-directories
     * 
     * @param fs
     * @param directory
     * @return true iff the directory has at least one sub-directory
     * @throws IOException
     */
    private static boolean hasSubDirectories(FileSystem fs, Path directory) throws IOException {
        FileStatus[] statuses = fs.listStatus(directory);

        if(statuses == null)
            return false;

        for(FileStatus status: statuses) {
            if(status != null && status.isDir() && !shouldPathBeIgnored(status.getPath())) {
                // we have found a subDirectory
                return true;
            }
        }
        // we are done looping through the directory and found no subDirectories
        return false;
    }

    public static JobConf addAllSubPaths(JobConf conf, Path path) throws IOException {
        if(shouldPathBeIgnored(path)) {
            throw new IllegalArgumentException(String.format("Path[%s] should be ignored.", path));
        }

        final FileSystem fs = path.getFileSystem(conf);

        if(fs.exists(path)) {
            for(FileStatus status: fs.listStatus(path)) {
                if(!shouldPathBeIgnored(status.getPath())) {
                    if(status.isDir()) {
                        addAllSubPaths(conf, status.getPath());
                    } else {
                        FileInputFormat.addInputPath(conf, status.getPath());
                    }
                }
            }
        }

        return conf;
    }

    /**
     * Check if the path should be ignored. Currently only paths with "_log" are
     * ignored.
     * 
     * @param path
     * @return
     * @throws IOException
     */
    public static boolean shouldPathBeIgnored(Path path) throws IOException {
        return path.getName().startsWith("_");
    }

    public static Map<String, String> getMapByPrefix(Configuration conf, String prefix) {
        Map<String, String> values = new HashMap<String, String>();
        for(Entry<String, String> entry: conf) {
            if(entry.getKey().startsWith(prefix))
                values.put(entry.getKey().substring(prefix.length()), entry.getValue());
        }
        return values;
    }

    public static void saveProps(Props props, String file) throws IOException {
        Path path = new Path(file);

        FileSystem fs = null;
        if(props.containsKey("hadoop.job.ugi")) {
            fs = getFileSystem(props);
        } else {
            fs = path.getFileSystem(new Configuration());
        }

        saveProps(fs, props, file);
    }

    public static void saveProps(FileSystem fs, Props props, String file) throws IOException {
        Path path = new Path(file);

        // create directory if it does not exist.
        Path parent = path.getParent();
        if(!fs.exists(parent))
            fs.mkdirs(parent);

        // write out properties
        OutputStream output = fs.create(path);
        try {
            props.storeFlattened(output);
        } finally {
            output.close();
        }
    }

    public static Props readProps(String file) throws IOException {
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(new Configuration());
        if(fs.exists(path)) {
            InputStream input = fs.open(path);
            try {
                // wrap it up in another layer so that the user can override
                // properties
                Props p = new Props(null, input);
                return new Props(p);
            } finally {
                input.close();
            }
        } else {
            return new Props();
        }
    }

    public static String readAsString(Path path) {
        InputStream input = null;
        try {
            FileSystem fs = path.getFileSystem(new Configuration());
            input = fs.open(path);
            return IOUtils.toString(input);
        } catch(IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(input);
        }
    }

    public static boolean mkdirs(String pathName) throws IOException {
        Path path = new Path(pathName);
        FileSystem fs = path.getFileSystem(new Configuration());
        return fs.mkdirs(path);
    }

    public static void deletePathIfExists(JobConf conf, String stepOutputPath) throws IOException {
        Path path = new Path(stepOutputPath);
        FileSystem fs = path.getFileSystem(conf);
        if(fs.exists(path)) {
            fs.delete(path, true);
        }
    }

    /**
     * Tag the BytesWritable with an integer at the END
     */
    public static void appendTag(BytesWritable writable, int tag) {
        int size = writable.getLength();

        if(writable.getCapacity() < size + 4) {
            // BytesWritable preserves old values
            writable.setCapacity(size + 4);
        }

        ByteUtils.writeInt(writable.getBytes(), tag, size);
        writable.setSize(size + 4);
    }

    /**
     * read and return integer from the END of BytesWritable The tag bytes are
     * NOT removed
     */
    public static int readTag(BytesWritable readable) {
        return ByteUtils.readInt(readable.getBytes(), readable.getLength() - 4);
    }

    /**
     * creates default data for given schema is needed for mappers/reducers
     * which tries to handle different schema.
     * 
     * Outputs<br>
     * <br>
     * Map : outputs default value for each subType <br>
     * List : output empty list <br>
     * JsonTypes: default 0 or '' empty strings
     */
    public static Object createDefaultData(Object typeSchema) {
        if(typeSchema instanceof List<?>) {
            ArrayList<Object> list = new ArrayList<Object>(0);
            return list;
        } else if(typeSchema instanceof Map<?, ?>) {
            HashMap<String, Object> map = new HashMap<String, Object>();
            for(Map.Entry<String, Object> typeEntry: ((Map<String, Object>) typeSchema).entrySet()) {
                map.put(typeEntry.getKey(), createDefaultData(typeEntry.getValue()));
            }
            return map;
        } else if(typeSchema instanceof JsonTypes) {
            return createDefaultJsonData((JsonTypes) typeSchema);
        }

        throw new RuntimeException("Invlaid schema type:" + typeSchema);
    }

    private static Object createDefaultJsonData(JsonTypes type) {

        if(JsonTypes.BOOLEAN.equals(type))
            return false;
        else if(JsonTypes.DATE.equals(type))
            return new Date();
        else if(JsonTypes.FLOAT32.equals(type) || JsonTypes.FLOAT64.equals(type)
                || JsonTypes.INT8.equals(type) || JsonTypes.INT16.equals(type)
                || JsonTypes.INT32.equals(type))
            return 0;
        else if(JsonTypes.BYTES.equals(type)) {
            byte[] data = new byte[0];
            return data;
        } else if(JsonTypes.STRING.equals(type)) {
            return "";
        }

        throw new RuntimeException("Invalid JsonType:" + type);
    }

    /**
     * Looks for the latest (the alphabetically greatest) path contained in the
     * given directory that passes the specified regex pattern.
     * 
     * @param fs The file system
     * @param directory The directory that will contain the versions
     * @param acceptRegex The String pattern
     * @return
     * @throws IOException
     */
    public static Path getLatestVersionedPath(FileSystem fs, Path directory, String acceptRegex)
            throws IOException {
        final String pattern = acceptRegex != null ? acceptRegex : "\\S+";

        PathFilter filter = new PathFilter() {

            @Override
            public boolean accept(Path arg0) {
                return !arg0.getName().startsWith("_") && Pattern.matches(pattern, arg0.getName());
            }
        };

        FileStatus[] statuses = fs.listStatus(directory, filter);

        if(statuses == null || statuses.length == 0) {
            return null;
        }

        Arrays.sort(statuses);

        return statuses[statuses.length - 1].getPath();
    }

    /**
     * Looks for the latest (the alphabetically greatest) path contained in the
     * given directory that passes the specified regex pattern "\\S+" for all
     * non spaced words.
     * 
     * @param fs The file system
     * @param directory The directory that will contain the versions
     * @return
     * @throws IOException
     */
    public static Path getLatestVersionedPath(FileSystem fs, Path directory) throws IOException {
        return getLatestVersionedPath(fs, directory, null);
    }

    /**
     * Does the same thing as getLatestVersionedPath, but checks to see if the
     * directory contains #LATEST. If it doesn't, it just returns what was
     * passed in.
     * 
     * @param fs
     * @param directory
     * @return
     * @throws IOException
     */
    public static Path getSanitizedPath(FileSystem fs, Path directory, String acceptRegex)
            throws IOException {
        if(directory.getName().endsWith("#LATEST")) {
            // getparent strips out #LATEST
            return getLatestVersionedPath(fs, directory.getParent(), acceptRegex);
        }

        return directory;
    }

    public static Path getSanitizedPath(Path path) throws IOException {
        return getSanitizedPath(path.getFileSystem(new Configuration()), path);
    }

    /**
     * Does the same thing as getLatestVersionedPath, but checks to see if the
     * directory contains #LATEST. If it doesn't, it just returns what was
     * passed in.
     * 
     * @param fs
     * @param directory
     * @return
     * @throws IOException
     */
    public static Path getSanitizedPath(FileSystem fs, Path directory) throws IOException {
        if(directory.getName().endsWith("#LATEST")) {
            // getparent strips out #LATEST
            return getLatestVersionedPath(fs, directory.getParent(), null);
        }

        return directory;
    }

    /**
     * Easily cleans up old data (alphabetically least) paths that is accepted
     * by the regex.
     * 
     * @param fs The file system
     * @param directory The directory that will contain the versions
     * @param acceptRegex The String pattern
     * @param backupNumber The number of versions we should keep. Otherwise
     *        we'll clean up.
     * @return
     * @throws IOException
     */
    public static void cleanupOlderVersions(FileSystem fs,
                                            Path directory,
                                            final String acceptRegex,
                                            int backupNumber) throws IOException {
        if(backupNumber < 1) {
            logger.error("Number of versions must be 1 or greater");
            return;
        }

        PathFilter filter = new PathFilter() {

            @Override
            public boolean accept(Path arg0) {
                return !arg0.getName().startsWith("_")
                       && Pattern.matches(acceptRegex, arg0.getName());
            }
        };

        FileStatus[] statuses = fs.listStatus(directory, filter);
        if(statuses == null) {
            logger.info("No backup files found");
            return;
        }

        Arrays.sort(statuses);

        int lastIndex = statuses.length - backupNumber;
        for(int i = 0; i < lastIndex; ++i) {
            logger.info("Deleting " + statuses[i].getPath());
            fs.delete(statuses[i].getPath(), true);
        }
    }

    public static void cleanupOlderVersions(FileSystem fs, Path directory, int backupNumber)
            throws IOException {
        cleanupOlderVersions(fs, directory, "\\S+", backupNumber);
    }

    /**
     * Move the file from one place to another. Unlike the raw Hadoop API this
     * will throw an exception if it fails. Like the Hadoop api it will fail if
     * a file exists in the destination.
     * 
     * @param fs The filesystem
     * @param from The source file to move
     * @param to The destination location
     * @throws IOException
     */
    public static void move(FileSystem fs, Path from, Path to) throws IOException {
        boolean success = fs.rename(from, to);
        if(!success)
            throw new RuntimeException("Failed to move " + from + " to " + to);
    }

    /**
     * Move the give file to the given location. Delete any existing file in
     * that location. Use the temp directory to make the operation as
     * transactional as possible. Throws an exception if the move fails.
     * 
     * @param fs The filesystem
     * @param from The source file
     * @param to The destination file
     * @param temp A temp directory to use
     * @throws IOException
     */
    public static void replaceFile(FileSystem fs, Path from, Path to, Path temp) throws IOException {
        fs.delete(temp, true);
        move(fs, to, temp);
        try {
            move(fs, from, to);
            fs.delete(temp, true);
        } catch(IOException e) {
            // hmm something went wrong, attempt to restore
            fs.rename(temp, to);
            throw e;
        }
    }
}
