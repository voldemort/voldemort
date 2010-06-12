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

package voldemort.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import voldemort.store.readonly.mr.HadoopStoreJobRunner;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

public class VoldemortWordCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new VoldemortWordCount(), args);
        System.exit(res);
    }

    public static class SimpleTokenizer extends
            Mapper<ByteArray, Versioned<byte[]>, Text, IntWritable> {

        private Text word = new Text();

        @Override
        public void map(ByteArray key, Versioned<byte[]> value, Context context)
                throws IOException, InterruptedException {
            String valueString = new String(value.getValue());
            StringTokenizer itr = new StringTokenizer(valueString);
            while(itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, new IntWritable(1));
            }
        }

    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val: values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public int run(String[] args) throws Exception {

        if(args.length != 2) {
            Utils.croak("USAGE: java VoldemortWordCount store_name adminclient_bootstrap_url");
        }

        Configuration conf = getConf();

        Class[] deps = new Class[] { voldemort.cluster.Node.class,
                voldemort.hadoop.VoldemortInputSplit.class,
                voldemort.hadoop.VoldemortInputFormat.class,
                voldemort.hadoop.VoldemortHadoopConfig.class, com.google.protobuf.Message.class,
                org.jdom.Content.class, com.google.common.collect.ImmutableList.class,
                org.apache.commons.io.IOUtils.class, org.apache.hadoop.mapreduce.RecordReader.class };

        String storeName = args[0];
        String url = args[1];

        Job job = new Job(conf, "wordcount");
        job.setJarByClass(getClass());
        job.setMapperClass(SimpleTokenizer.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        addDepJars(job.getConfiguration(), deps);

        job.setInputFormatClass(VoldemortInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("wordcount" + System.nanoTime()));

        VoldemortHadoopConfig.setVoldemortURL(job.getConfiguration(), url);
        VoldemortHadoopConfig.setVoldemortStoreName(job.getConfiguration(), storeName);

        job.waitForCompletion(true);
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

    private static void addDepJars(Configuration conf, Class<?>[] deps) throws IOException {
        FileSystem localFs = FileSystem.getLocal(conf);
        Set<String> depJars = new HashSet<String>();
        for(Class<?> dep: deps) {
            String tmp = findInClasspath(dep.getCanonicalName());
            if(tmp != null) {
                Path path = new Path(tmp);
                depJars.add(path.makeQualified(localFs).toString());
            }
        }

        String[] tmpjars = conf.get("tmpjars", "").split(",");
        for(String tmpjar: tmpjars) {
            if(!StringUtils.isEmpty(tmpjar)) {
                depJars.add(tmpjar.trim());
            }
        }
        conf.set("tmpjars", StringUtils.join(depJars.iterator(), ','));
    }
}
