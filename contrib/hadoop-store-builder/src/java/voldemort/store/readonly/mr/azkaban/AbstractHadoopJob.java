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

package voldemort.store.readonly.mr.azkaban;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import voldemort.store.readonly.mr.utils.HadoopUtils;
import voldemort.utils.Props;

/**
 * An abstract Base class for Hadoop Jobs
 * 
 * @author bbansal
 * 
 */
public abstract class AbstractHadoopJob /*extends AbstractJob*/ {

    public static String COMMON_FILE_DATE_PATTERN = "yyyy-MM-dd-HH-mm";
    public static final String HADOOP_PREFIX = "hadoop-conf.";
    public static final String LATEST_SUFFIX = "#LATEST";
    public static final String CURRENT_SUFFIX = "#CURRENT";
    protected final Props props;
    private RunningJob _runningJob;

    private final static String voldemortLibPath = "voldemort.distributedcache";

    private final static String hadoopLibPath = "hdfs.default.classpath.dir";

    private final String _id;
    protected final Logger _log;

    public AbstractHadoopJob(String name, Props props) {
        this(name, props, Logger.getLogger(AbstractHadoopJob.class.getName()));
    }

    public AbstractHadoopJob(String name, Props props, Logger logger) {
        this._id = name;
        this._log = logger;
        this.props = props;
    }

    public String getId() {
        return _id;
    }

    public void debug(String message) {
        this._log.debug(message);
    }

    public void debug(String message, Throwable t) {
        this._log.debug(message, t);
    }

    public void info(String message) {
        this._log.info(message);
    }

    public void info(String message, Throwable t) {
        this._log.info(message, t);
    }

    public void warn(String message) {
        this._log.warn(message);
    }

    public void warn(String message, Throwable t) {
        this._log.warn(message, t);
    }

    public void error(String message) {
        this._log.error(message);
    }

    public void error(String message, Throwable t) {
        this._log.error(message, t);
    }

    public void run(JobConf conf) throws Exception {
        _runningJob = new JobClient(conf).submitJob(conf);
        info("See " + _runningJob.getTrackingURL() + " for details.");
        _runningJob.waitForCompletion();

        if(!_runningJob.isSuccessful()) {
            throw new Exception("Hadoop job:" + getId() + " failed!");
        }

        // dump all counters
        Counters counters = _runningJob.getCounters();
        for(String groupName: counters.getGroupNames()) {
            Counters.Group group = counters.getGroup(groupName);
            info("Group: " + group.getDisplayName());
            for(Counter counter: group)
                info(counter.getDisplayName() + ":\t" + counter.getValue());
        }
    }

    public JobConf createJobConf() throws IOException,
            URISyntaxException {
        JobConf conf = new JobConf();
        // set custom class loader with custom find resource strategy.

        conf.setJobName(getId());
        conf.setNumReduceTasks(0);

        String hadoop_ugi = props.getString("hadoop.job.ugi", null);
        if(hadoop_ugi != null) {
            conf.set("hadoop.job.ugi", hadoop_ugi);
        }

        if(props.getBoolean("is.local", false)) {
            conf.set("mapred.job.tracker", "local");
            conf.set("fs.default.name", "file:///");
            conf.set("mapred.local.dir", "/tmp/map-red");

            info("Running locally, no hadoop jar set.");
        } else {
            setClassLoaderAndJar(conf, getClass());
            info("Setting hadoop jar file for class:" + getClass() + "  to " + conf.getJar());
            info("*************************************************************************");
            info("          Running on Real Hadoop Cluster(" + conf.get("mapred.job.tracker")
                 + ")           ");
            info("*************************************************************************");
        }

        // set JVM options if present
        if(props.containsKey("mapred.child.java.opts")) {
            conf.set("mapred.child.java.opts", props.getString("mapred.child.java.opts"));
            info("mapred.child.java.opts set to " + props.getString("mapred.child.java.opts"));
        }

        // set input and output paths if they are present
        if(props.containsKey("input.paths")) {
            List<String> inputPaths = props.getList("input.paths");
            if(inputPaths.size() == 0)
                throw new IllegalArgumentException("Must specify at least one value for property 'input.paths'");
            for(String path: inputPaths) {
                // Implied stuff, but good implied stuff
                if(path.endsWith(LATEST_SUFFIX)) {
                    FileSystem fs = FileSystem.get(conf);

                    PathFilter filter = new PathFilter() {

                        @Override
                        public boolean accept(Path arg0) {
                            return !arg0.getName().startsWith("_")
                                   && !arg0.getName().startsWith(".");
                        }
                    };

                    String latestPath = path.substring(0, path.length() - LATEST_SUFFIX.length());
                    FileStatus[] statuses = fs.listStatus(new Path(latestPath), filter);

                    Arrays.sort(statuses);

                    path = statuses[statuses.length - 1].getPath().toString();
                    System.out.println("Using latest folder: " + path);
                }
                HadoopUtils.addAllSubPaths(conf, new Path(path));
            }
        }

        if(props.containsKey("output.path")) {
            String location = props.get("output.path");
            if(location.endsWith("#CURRENT")) {
                DateTimeFormatter format = DateTimeFormat.forPattern(COMMON_FILE_DATE_PATTERN);
                String destPath = format.print(new DateTime());
                location = location.substring(0, location.length() - "#CURRENT".length())
                           + destPath;
                System.out.println("Store location set to " + location);
            }

            FileOutputFormat.setOutputPath(conf, new Path(location));
            // For testing purpose only remove output file if exists
            if(props.getBoolean("force.output.overwrite", false)) {
                FileSystem fs = FileOutputFormat.getOutputPath(conf).getFileSystem(conf);
                fs.delete(FileOutputFormat.getOutputPath(conf), true);
            }
        }

        // Adds External jars to hadoop classpath
        String externalJarList = props.getString("hadoop.external.jarFiles", null);
        if(externalJarList != null) {
            String[] jarFiles = externalJarList.split(",");
            for(String jarFile: jarFiles) {
                info("Adding extenral jar File:" + jarFile);
                DistributedCache.addFileToClassPath(new Path(jarFile), conf);
            }
        }

        // Adds distributed cache files
        String cacheFileList = props.getString("hadoop.cache.files", null);
        if(cacheFileList != null) {
            String[] cacheFiles = cacheFileList.split(",");
            for(String cacheFile: cacheFiles) {
                info("Adding Distributed Cache File:" + cacheFile);
                DistributedCache.addCacheFile(new URI(cacheFile), conf);
            }
        }

        // Adds distributed cache files
        String archiveFileList = props.getString("hadoop.cache.archives", null);
        if(archiveFileList != null) {
            String[] archiveFiles = archiveFileList.split(",");
            for(String archiveFile: archiveFiles) {
                info("Adding Distributed Cache Archive File:" + archiveFile);
                DistributedCache.addCacheArchive(new URI(archiveFile), conf);
            }
        }

        // this property can be set by azkaban to manage voldemort lib path on
        // hdfs
        addToDistributedCache(voldemortLibPath, conf);

        boolean isAddFiles = props.getBoolean("hdfs.default.classpath.dir.enable", false);
        if(isAddFiles) {
            addToDistributedCache(hadoopLibPath, conf);
        }

        // May want to add this to HadoopUtils, but will await refactoring
        for(String key: getProps().keySet()) {
            String lowerCase = key.toLowerCase();
            if(lowerCase.startsWith(HADOOP_PREFIX)) {
                String newKey = key.substring(HADOOP_PREFIX.length());
                conf.set(newKey, getProps().get(key));
            }
        }

        // Add properties specific to reducer output compression
        conf.setBoolean(VoldemortBuildAndPushJob.REDUCER_OUTPUT_COMPRESS,
                        props.getBoolean(VoldemortBuildAndPushJob.REDUCER_OUTPUT_COMPRESS, false));
        if(props.containsKey(VoldemortBuildAndPushJob.REDUCER_OUTPUT_COMPRESS_CODEC)) {
            conf.set(VoldemortBuildAndPushJob.REDUCER_OUTPUT_COMPRESS_CODEC,
                     props.get(VoldemortBuildAndPushJob.REDUCER_OUTPUT_COMPRESS_CODEC));
        }

        HadoopUtils.setPropsInJob(conf, getProps());

        // http://hadoop.apache.org/docs/r1.1.1/mapred_tutorial.html#Job+Credentials

        // The MapReduce tokens are provided so that tasks can spawn jobs if
        // they wish to.
        // The tasks authenticate to the JobTracker via the MapReduce delegation
        // tokens.
        if(System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            conf.set("mapreduce.job.credentials.binary",
                     System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
        }
        return conf;
    }

    /*
     * Loads jar files into distributed cache This way the mappers and reducers
     * have the jars they need at run time
     */
    private void addToDistributedCache(String propertyName, JobConf conf) throws IOException {
        String jarDir = props.getString(propertyName, null);
        if(jarDir != null) {
            FileSystem fs = FileSystem.get(conf);
            if(fs != null) {
                FileStatus[] status = fs.listStatus(new Path(jarDir));

                if(status != null) {
                    for(int i = 0; i < status.length; ++i) {
                        if(!status[i].isDir()) {
                            Path path = new Path(jarDir, status[i].getPath().getName());
                            info("Adding Jar to Distributed Cache Archive File:" + path);

                            DistributedCache.addFileToClassPath(path, conf);
                        }
                    }
                } else {
                    info(propertyName + jarDir + " is empty.");
                }
            } else {
                info(propertyName + jarDir + " filesystem doesn't exist");
            }
        }

    }

    public Props getProps() {
        return this.props;
    }

    public void cancel() throws Exception {
        if(_runningJob != null)
            _runningJob.killJob();
    }

    public double getProgress() throws IOException {
        if(_runningJob == null)
            return 0.0;
        else
            return (double) (_runningJob.mapProgress() + _runningJob.reduceProgress()) / 2.0d;
    }

    public Counters getCounters() throws IOException {
        return _runningJob.getCounters();
    }

    public static void setClassLoaderAndJar(JobConf conf, Class jobClass) {
        conf.setClassLoader(Thread.currentThread().getContextClassLoader());
        String jar = HadoopUtils.findContainingJar(jobClass, Thread.currentThread()
                                                                   .getContextClassLoader());
        if(jar != null) {
            conf.setJar(jar);
        }
    }
}
