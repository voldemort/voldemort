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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import joptsimple.OptionSet;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import voldemort.utils.CmdUtils;
import voldemort.utils.HostNamePair;
import voldemort.utils.RemoteOperation;
import voldemort.utils.RemoteOperationException;
import voldemort.utils.impl.SshClusterStarter;
import voldemort.utils.impl.SshClusterStopper;

public class VoldemortClusterStarterApp extends VoldemortApp {

    public static void main(String[] args) throws Exception {
        new VoldemortClusterStarterApp().run(args);
    }

    @Override
    protected String getScriptName() {
        return "voldemort-clusterstarter.sh";
    }

    @Override
    public void run(String[] args) throws Exception {
        parser.accepts("help", "Prints this help");
        parser.accepts("logging",
                       "Options are \"debug\", \"info\" (default), \"warn\", \"error\", or \"off\"")
              .withRequiredArg();
        parser.accepts("hostnames", "File containing host names").withRequiredArg();
        parser.accepts("sshprivatekey", "File containing SSH private key (optional)")
              .withRequiredArg();
        parser.accepts("hostuserid", "User ID on remote host").withRequiredArg();
        parser.accepts("voldemortroot", "Voldemort's root directory on remote host")
              .withRequiredArg();
        parser.accepts("voldemorthome", "Voldemort's home directory on remote host")
              .withRequiredArg();
        parser.accepts("clusterxml",
                       "Voldemort's cluster.xml file on the local file system; used to determine host names")
              .withRequiredArg();

        OptionSet options = parse(args);
        File hostNamesFile = getRequiredInputFile(options, "hostnames");

        List<HostNamePair> hostNamePairs = getHostNamesPairsFromFile(hostNamesFile);
        final List<String> externalHostNames = new ArrayList<String>();
        final List<String> internalHostNames = new ArrayList<String>();

        for(HostNamePair hostNamePair: hostNamePairs) {
            externalHostNames.add(hostNamePair.getExternalHostName());
            internalHostNames.add(hostNamePair.getInternalHostName());
        }

        final File sshPrivateKey = getInputFile(options, "sshprivatekey");
        final String hostUserId = CmdUtils.valueOf(options, "hostuserid", "root");
        String voldemortHomeDirectory = getRequiredString(options, "voldemorthome");
        final String voldemortRootDirectory = getRequiredString(options, "voldemortroot");

        File clusterXmlFile = getRequiredInputFile(options, "clusterxml");

        Map<String, Integer> nodeIds = getNodeIds(hostNamesFile, clusterXmlFile, hostNamePairs);

        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                RemoteOperation operation = new SshClusterStopper(externalHostNames,
                                                                  sshPrivateKey,
                                                                  hostUserId,
                                                                  voldemortRootDirectory);
                try {
                    operation.execute();
                } catch(RemoteOperationException e) {
                    e.printStackTrace();
                }
            }

        });

        RemoteOperation operation = new SshClusterStarter(externalHostNames,
                                                          sshPrivateKey,
                                                          hostUserId,
                                                          voldemortRootDirectory,
                                                          voldemortHomeDirectory,
                                                          nodeIds);
        operation.execute();
    }

    private Map<String, Integer> getNodeIds(File hostNamesFile,
                                            File clusterXmlFile,
                                            List<HostNamePair> hostNamePairs) throws Exception {
        Map<String, Integer> nodeIds = new HashMap<String, Integer>();
        DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document document = documentBuilder.parse(clusterXmlFile);

        NodeList documentChildren = document.getChildNodes().item(0).getChildNodes();

        for(int i = 0; i < documentChildren.getLength(); i++) {
            Node documentChild = documentChildren.item(i);

            if(documentChild.getNodeName().equals("server")) {
                NodeList serverChildren = documentChild.getChildNodes();
                String internalHostName = null;
                String id = null;

                for(int j = 0; j < serverChildren.getLength(); j++) {
                    Node serverChild = serverChildren.item(j);

                    if(serverChild.getNodeName().equals("host"))
                        internalHostName = serverChild.getTextContent();
                    else if(serverChild.getNodeName().equals("id"))
                        id = serverChild.getTextContent();
                }

                if(internalHostName != null && id != null) {
                    // Yes, this is super inefficient, but we have to assign the
                    // node ID to the *external* host name but the cluster.xml
                    // file contains the *internal* host name. So loop over all
                    // the external->internal mappings and find the one that
                    // matches the internal host name and assign the node ID to
                    // its corresponding external host name.
                    for(HostNamePair hostNamePair: hostNamePairs) {
                        if(hostNamePair.getInternalHostName().equals(internalHostName))
                            nodeIds.put(hostNamePair.getExternalHostName(), Integer.parseInt(id));
                    }
                } else {
                    throw new Exception(clusterXmlFile.getAbsolutePath()
                                        + " appears to be corrupt; could not determine the <host> and/or <id> values for the <server> node");
                }
            }
        }

        if(nodeIds.size() != hostNamePairs.size()) {
            throw new Exception(clusterXmlFile.getAbsolutePath()
                                + " appears to be corrupt; not all of the hosts from "
                                + hostNamesFile.getAbsolutePath() + " were represented in "
                                + clusterXmlFile.getAbsolutePath());
        }

        return nodeIds;
    }
}
