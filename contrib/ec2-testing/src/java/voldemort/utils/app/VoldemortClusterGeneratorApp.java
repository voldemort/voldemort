/*
 * Copyright 2009 LinkedIn, Inc.
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

package voldemort.utils.app;

import java.io.File;
import java.util.List;

import joptsimple.OptionSet;

import org.apache.commons.io.FileUtils;

import voldemort.utils.ClusterGenerator;
import voldemort.utils.CmdUtils;

public class VoldemortClusterGeneratorApp extends VoldemortApp {

    public static void main(String[] args) throws Exception {
        new VoldemortClusterGeneratorApp().run(args);
    }

    @Override
    protected String getScriptName() {
        return "voldemort-clustergenerator.sh";
    }

    @Override
    public void run(String[] args) throws Exception {
        parser.accepts("help", "Prints this help");
        parser.accepts("logging",
                       "Options are \"debug\", \"info\", \"warn\" (default), \"error\", or \"off\"")
              .withRequiredArg();
        parser.accepts("hostnames", "File containing host names").withRequiredArg();
        parser.accepts("partitions", "Number of partitions per cluster node")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("output", "cluster.xml configuration file; defaults to stdout")
              .withRequiredArg();
        parser.accepts("clustername", "Cluster name; defaults to mycluster").withRequiredArg();

        OptionSet options = parse(args);
        File hostNamesFile = getRequiredInputFile(options, "hostnames");
        int partitions = getRequiredInt(options, "partitions");
        File outputFile = getOutputFile(options, "output");
        String clusterName = CmdUtils.valueOf(options, "clustername", "mycluster");

        List<String> privateHostNames = getHostNamesFromFile(hostNamesFile, false);

        String clusterXml = new ClusterGenerator().createClusterDescriptor(clusterName,
                                                                           privateHostNames,
                                                                           partitions);

        if(outputFile != null)
            FileUtils.writeStringToFile(outputFile, clusterXml);
        else
            System.out.print(clusterXml);
    }

}
