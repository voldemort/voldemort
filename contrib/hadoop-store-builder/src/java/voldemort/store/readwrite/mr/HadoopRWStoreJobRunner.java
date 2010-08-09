/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.store.readwrite.mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jdom.JDOMException;

import voldemort.cluster.Cluster;
import voldemort.server.VoldemortConfig;
import voldemort.store.StoreDefinition;
import voldemort.utils.CmdUtils;
import voldemort.utils.ReflectUtils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableCollection;

/**
 * A runner class to facilitate the launching of HadoopStoreBuilder from the
 * command-line.
 */
@SuppressWarnings("deprecation")
public class HadoopRWStoreJobRunner extends Configured implements Tool {

    private static void printUsage(OptionParser parser, Exception e) throws IOException {
        System.err.println("Usage: $VOLDEMORT_HOME/bin/hadoop-build-readwrite-store.sh \\");
        System.err.println("          [genericOptions] [options]\n");
        System.err.println("Options:");
        parser.printHelpOn(System.err);
        System.err.println();
        ToolRunner.printGenericCommandUsage(System.err);

        if(e != null) {
            System.err.println("\nAn exception ocurred:");
            e.printStackTrace(System.err);
        }
    }

    private static OptionParser configureParser() {
        OptionParser parser = new OptionParser();
        parser.accepts("input", "input file(s) for the Map step.").withRequiredArg();
        parser.accepts("temp", "temporary directory for the Reduce dump.").withRequiredArg();
        parser.accepts("mapper", "store builder mapper class.").withRequiredArg();
        parser.accepts("cluster", "local path to cluster.xml.").withRequiredArg();
        parser.accepts("storedefinitions", "local path to stores.xml.").withRequiredArg();
        parser.accepts("storename", "store name from store definition.").withRequiredArg();
        parser.accepts("inputformat", "JavaClassName (default=text).").withRequiredArg();
        parser.accepts("reducerspernode", "number of reducers per node (default=1)")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("jar", "mapper class jar if not in $HADOOP_CLASSPATH.").withRequiredArg();
        parser.accepts("hadoopnodeid", "node id for hadoop (default=num_nodes+1)")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("pushversion", "version of push (default=1)")
              .withRequiredArg()
              .ofType(Long.class);
        parser.accepts("help", "print usage information");
        return parser;
    }

    @SuppressWarnings("unchecked")
    public int run(String[] args) throws Exception {

        OptionParser parser = configureParser();
        OptionSet options = parser.parse(args);

        if(options.has("help")) {
            printUsage(parser, null);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options,
                                               "input",
                                               "temp",
                                               "mapper",
                                               "cluster",
                                               "storedefinitions",
                                               "storename");

        if(missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing)
                               + "\n");
            printUsage(parser, null);
            System.exit(1);
        }

        File clusterFile = new File((String) options.valueOf("cluster"));
        Cluster cluster = new ClusterMapper().readCluster(new BufferedReader(new FileReader(clusterFile)));
        File storeDefFile = new File((String) options.valueOf("storedefinitions"));
        String storeName = (String) options.valueOf("storename");
        Path inputPath = new Path((String) options.valueOf("input"));
        Path tempPath = new Path((String) options.valueOf("temp"));

        List<StoreDefinition> stores;
        stores = new StoreDefinitionsMapper().readStoreList(new BufferedReader(new FileReader(storeDefFile)));
        StoreDefinition storeDef = null;
        for(StoreDefinition def: stores) {
            if(def.getName().equals(storeName))
                storeDef = def;
        }

        if(storeDef == null) {
            System.err.println("Missing store definition for store name '" + storeName + "'");
            printUsage(parser, null);
            System.exit(1);
        }

        List<String> addJars = new ArrayList<String>();
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if(options.has("jar")) {
            String jar = (String) options.valueOf("jar");
            URL[] urls = new URL[1];
            urls[0] = new File(jar).toURI().toURL();
            cl = new URLClassLoader(urls);
            addJars.add(jar);
        }

        Class<? extends AbstractRWHadoopStoreBuilderMapper<?, ?>> mapperClass = (Class<? extends AbstractRWHadoopStoreBuilderMapper<?, ?>>) ReflectUtils.loadClass((String) options.valueOf("mapper"),
                                                                                                                                                                   cl);

        Class<? extends InputFormat<?, ?>> inputFormatClass = TextInputFormat.class;
        if(options.has("inputformat")) {
            String inputFormatClassName = (String) options.valueOf("inputformat");
            if(!inputFormatClassName.equalsIgnoreCase("TextInputFormat")) {
                inputFormatClass = (Class<? extends InputFormat<?, ?>>) ReflectUtils.loadClass(inputFormatClassName,
                                                                                               cl);
            }
        }

        if(inputFormatClass == null) {
            inputFormatClass = TextInputFormat.class;
        }

        int hadoopNodeId;
        if(options.has("hadoopnodeid")) {
            hadoopNodeId = (Integer) options.valueOf("hadoopnodeid");
        } else {
            hadoopNodeId = (short) cluster.getNumberOfNodes();
        }

        int reducersPerNode;
        if(options.has("reducerspernode")) {
            reducersPerNode = (Integer) options.valueOf("reducerspernode");
        } else {
            reducersPerNode = 1;
        }

        long pushVersion;
        if(options.has("pushversion")) {
            pushVersion = (Long) options.valueOf("pushversion");
        } else {
            pushVersion = 1L;
        }

        Configuration conf = getConf();

        Class[] deps = new Class[] { ImmutableCollection.class, JDOMException.class,
                VoldemortConfig.class, HadoopRWStoreJobRunner.class, mapperClass,
                com.google.protobuf.Message.class, org.jdom.Content.class,
                com.google.common.collect.ImmutableList.class, org.apache.commons.io.IOUtils.class };

        addDepJars(conf, deps, addJars);

        HadoopRWStoreBuilder builder = new HadoopRWStoreBuilder(conf,
                                                                mapperClass,
                                                                inputFormatClass,
                                                                cluster,
                                                                storeDef,
                                                                reducersPerNode,
                                                                hadoopNodeId,
                                                                pushVersion,
                                                                tempPath,
                                                                inputPath);

        builder.build();
        return 0;
    }

    public static String findInClasspath(String className) {
        return findInClasspath(className, HadoopRWStoreJobRunner.class.getClassLoader());
    }

    /**
     * @return a jar file path or a base directory or null if not found.
     */
    public static String findInClasspath(String className, ClassLoader loader) {

        String relPath = className;
        relPath = relPath.replace('.', '/');
        relPath += ".class";
        java.net.URL classUrl = loader.getResource(relPath);

        String codePath;
        if(classUrl != null) {
            boolean inJar = classUrl.getProtocol().equals("jar");
            codePath = classUrl.toString();
            if(codePath.startsWith("jar:")) {
                codePath = codePath.substring("jar:".length());
            }
            if(codePath.startsWith("file:")) { // can have both
                codePath = codePath.substring("file:".length());
            }
            if(inJar) {
                // A jar spec: remove class suffix in
                // /path/my.jar!/package/Class
                int bang = codePath.lastIndexOf('!');
                codePath = codePath.substring(0, bang);
            } else {
                // A class spec: remove the /my/package/Class.class portion
                int pos = codePath.lastIndexOf(relPath);
                if(pos == -1) {
                    throw new IllegalArgumentException("invalid codePath: className=" + className
                                                       + " codePath=" + codePath);
                }
                codePath = codePath.substring(0, pos);
            }
        } else {
            codePath = null;
        }
        return codePath;
    }

    public static void addDepJars(Configuration conf, Class<?>[] deps, List<String> additionalJars)
            throws IOException {
        FileSystem localFs = FileSystem.getLocal(conf);
        Set<String> depJars = new HashSet<String>();
        for(Class<?> dep: deps) {
            String tmp = findInClasspath(dep.getCanonicalName());
            if(tmp != null) {
                Path path = new Path(tmp);
                depJars.add(path.makeQualified(localFs).toString());
            }
        }

        for(String additional: additionalJars) {
            Path path = new Path(additional);
            depJars.add(path.makeQualified(localFs).toString());
        }

        String[] tmpjars = conf.get("tmpjars", "").split(",");
        for(String tmpjar: tmpjars) {
            if(!StringUtils.isEmpty(tmpjar)) {
                depJars.add(tmpjar.trim());
            }
        }
        conf.set("tmpjars", StringUtils.join(depJars.iterator(), ','));
    }

    public static void main(String[] args) {
        int res;
        try {
            res = ToolRunner.run(new Configuration(), new HadoopRWStoreJobRunner(), args);
            System.exit(res);
        } catch(Exception e) {
            e.printStackTrace();
            System.err.print("\nTry '--help' for more information.");
            System.exit(1);
        }
    }
}
