package voldemort.store.readonly.mr;

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

import org.apache.commons.cli2.Argument;
import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
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
import voldemort.utils.ReflectUtils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.ImmutableCollection;

/**
 * A runner class to facitilate the launching of HadoopStoreBuilder from the
 * command-line.
 * 
 * @author elias
 * 
 */
public class HadoopStoreJobRunner extends Configured implements Tool {

    private DefaultOptionBuilder builder = new DefaultOptionBuilder("-", "-", false);
    private ArgumentBuilder argBuilder = new ArgumentBuilder();
    private Parser parser = new Parser();
    private Group allOptions;

    public HadoopStoreJobRunner() {
        super();
        setupOptions();
    }

    private Option createOption(String name) {
        return createOption(name, true);
    }

    private Option createOption(String name, boolean required) {
        Argument argument = argBuilder.withMinimum(1).withMaximum(1).create();
        return builder.withLongName(name).withArgument(argument).withRequired(required).create();
    }

    private void setupOptions() {

        Option input = createOption("input");
        Option output = createOption("output");
        Option tmpdir = createOption("tmpdir");
        Option jar = createOption("jar", false);
        Option mapper = createOption("mapper");
        Option cluster = createOption("cluster");
        Option stores = createOption("storedefinitions");
        Option store = createOption("storename");
        Option chunkSizeBytes = createOption("chunksize");
        Option replicationFactor = createOption("replication");
        Option inputformat = createOption("inputformat", false);

        allOptions = new GroupBuilder().withOption(input)
                                       .withOption(output)
                                       .withOption(tmpdir)
                                       .withOption(mapper)
                                       .withOption(jar)
                                       .withOption(inputformat)
                                       .withOption(cluster)
                                       .withOption(stores)
                                       .withOption(store)
                                       .withOption(chunkSizeBytes)
                                       .withOption(replicationFactor)
                                       .create();
        parser.setGroup(allOptions);
    }

    private static void printUsage(Exception e) {
        System.out.println("Usage: $VOLDEMORT_HOME/bin/hadoop-build-readonly-store.sh \\");
        System.out.println("          [genericOptions] [options]\n");
        System.out.println("Options:");
        System.out.println("-input                <path> DFS input file(s) for the Map step.");
        System.out.println("-output               <path> DFS output directory for the Reduce step.");
        System.out.println("-mapper               <JavaClassName> The store builder mapper class.");
        System.out.println("-cluster              <path> Local path to cluster.xml.");
        System.out.println("-storedefinitions     <path> Local path to stores.xml.");
        System.out.println("-storename            <spec> The name of the store used for the store definition.");
        System.out.println("-replicationfactor    <num> The replication factor to use.");
        System.out.println("-chunksize            <num> The maximum size of a chunk in bytes.");
        System.out.println("-inputformat          TextInputFormat(default)|JavaClassName Optional.");
        System.out.println("-jar                  <path> Optional. The local path to jar with mapper class if not already in $HADOOP_CLASSPATH.");

        System.out.println();
        ToolRunner.printGenericCommandUsage(System.out);

        if(e != null) {
            System.out.println("\nAn exception ocurred:");
            e.printStackTrace(System.out);
        }
    }

    public int run(String[] args) throws Exception {

        Class<? extends AbstractHadoopStoreBuilderMapper<?, ?>> mapperClass = null;
        Class<? extends InputFormat<?, ?>> inputFormatClass = null;
        Cluster cluster = null;

        StoreDefinition storeDef = null;
        int replicationFactor = 0;
        long chunkSizeBytes = -1;
        Path inputPath = null;
        Path outputDir = null;
        Path tempDir = null;

        CommandLine cmdLine = parser.parse(args);

        List<String> addJars = new ArrayList<String>();

        if(cmdLine != null) {
            inputPath = new Path((String) cmdLine.getValue("-input"));
            outputDir = new Path((String) cmdLine.getValue("-output"));
            tempDir = new Path((String) cmdLine.getValue("-tmpdir"));

            File clusterFile = new File((String) cmdLine.getValue("-cluster"));
            cluster = new ClusterMapper().readCluster(new BufferedReader(new FileReader(clusterFile)));

            File storeDefFile = new File((String) cmdLine.getValue("-storedefinitions"));
            String storeName = (String) cmdLine.getValue("-storename");
            List<StoreDefinition> stores;
            stores = new StoreDefinitionsMapper().readStoreList(new BufferedReader(new FileReader(storeDefFile)));
            for(StoreDefinition def: stores) {
                if(def.getName().equals(storeName))
                    storeDef = def;
            }

            chunkSizeBytes = Long.parseLong((String) cmdLine.getValue("-chunksize"));
            replicationFactor = Integer.parseInt((String) cmdLine.getValue("-replication"));

            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            if(cmdLine.hasOption("-jar")) {
                String jar = (String) cmdLine.getValue("-jar");
                URL[] urls = new URL[1];
                urls[0] = new File(jar).toURI().toURL();
                cl = new URLClassLoader(urls);
                addJars.add(jar);
            }
            mapperClass = (Class<? extends AbstractHadoopStoreBuilderMapper<?, ?>>) ReflectUtils.loadClass((String) cmdLine.getValue("-mapper"),
                                                                                                           cl);

            if(cmdLine.hasOption("-inputformat")
               && !((String) cmdLine.getValue("-inputformat")).equals("TextInputFormat")) {
                inputFormatClass = (Class<? extends InputFormat<?, ?>>) ReflectUtils.loadClass((String) cmdLine.getValue("-inputformat"),
                                                                                               cl);
            } else {
                inputFormatClass = TextInputFormat.class;
            }
        }

        Configuration conf = getConf();

        Class[] deps = new Class[] { ImmutableCollection.class, JDOMException.class,
                VoldemortConfig.class, HadoopStoreJobRunner.class, mapperClass };

        addDepJars(conf, deps, addJars);

        HadoopStoreBuilder builder = new HadoopStoreBuilder(conf,
                                                            mapperClass,
                                                            inputFormatClass,
                                                            cluster,
                                                            storeDef,
                                                            replicationFactor,
                                                            chunkSizeBytes,
                                                            tempDir,
                                                            outputDir,
                                                            inputPath);

        builder.build();
        return 0;
    }

    public static String findInClasspath(String className) {
        return findInClasspath(className, HadoopStoreJobRunner.class.getClassLoader());
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

    private static void addDepJars(Configuration conf, Class[] deps, List<String> additionalJars)
            throws IOException {
        FileSystem localFs = FileSystem.getLocal(conf);
        Set<String> depJars = new HashSet<String>();
        for(Class dep: deps) {
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
            res = ToolRunner.run(new Configuration(), new HadoopStoreJobRunner(), args);
            System.exit(res);
        } catch(Exception e) {
            printUsage(e);
            System.exit(1);
        }
    }
}
