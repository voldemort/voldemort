/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort;

import java.lang.System;
import java.io.OutputStream;
import java.io.InputStream;
import java.lang.Process;
import java.lang.ProcessBuilder;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;
import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.io.FileReader;
import java.io.Writer;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.io.FileUtils;

import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.client.protocol.admin.QueryKeyResult;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.versioning.Versioned;
import voldemort.utils.CmdUtils;
import voldemort.tools.Repartitioner;

class NamedZone {

    public String name;	    // name of the zone for range purposes
    public String prefix;   // used as the prefix of hostnames in the zone
    public int id;	    // assigned zone id

    NamedZone(String n, String p, int i) {
	name = n;
	prefix = p;
	id = i;
    }

    // LI-specific
    private static NamedZone all[] = {
	new NamedZone("prod-ltx1",  "ltx1", 0),
	new NamedZone("ela4",	    "ela4", 1),
	new NamedZone("prod-lva1",  "lva1", 2)
    };
    public static NamedZone getById(int id) {
	for (NamedZone nz: all) {
	    if (nz.id == id)
		return nz;
	}
	return null;
    }
    public static NamedZone getByPrefix(String prefix) {
	for (NamedZone nz: all) {
	    if (nz.prefix.equals(prefix))
		return nz;
	}
	return null;
    }
    public static NamedZone getByName(String name) {
	for (NamedZone nz: all) {
	    if (nz.name.equals(name))
		return nz;
	}
	return null;
    }
}

class ClusterBuilder {

    private ClusterMapper mapper;
    private Cluster cluster;

    ClusterBuilder(Cluster cl) {
	mapper = new ClusterMapper();
	cluster = cl;
    }
    ClusterBuilder() {
	this(new Cluster("default", new LinkedList<Node>(), new LinkedList<Zone>()));
    }

    public ClusterBuilder dump() {
	System.out.println("Cluster {");

	System.out.println("Zones");
	System.out.println("    id proximity");
	System.out.println("    -- ---------");
	for(Zone zone: cluster.getZones()) {
	    StringBuffer buf = new StringBuffer();
	    for(Integer proxZoneId: zone.getProximityList())
		buf.append(" ").append(proxZoneId);
	    System.out.format("    %2d %s\n", zone.getId(), buf.toString());
	}

	System.out.println("Nodes\n");
	System.out.println("    id zone hostname");
	System.out.println("    -- ---- --------");
	for (Node node: cluster.getNodes()) {
	    System.out.format("    %2d %4d %s\n", node.getId(), node.getZoneId(), node.getHost());
	}

	System.out.println("} Cluster");
	return this;
    }

    public ClusterBuilder verify() throws Exception {

	// Verify that no zone has an unknown zone id
	for(Zone zone: cluster.getZones()) {
	    if (NamedZone.getById(zone.getId()) == null)
		throw new Exception("Zone " + zone.getId() + " has unexpected zone id");
	}

	// Verify that no zone has duplicates in its proximity list
	for(Zone zone: cluster.getZones()) {
	    HashSet<Integer> seenProximates = new HashSet<Integer>();
	    for(Integer proxZoneId: zone.getProximityList()) {
		Integer id = new Integer(proxZoneId);
		if (seenProximates.contains(id))
		    throw new Exception("Zone " + zone.getId() + " has duplicate proximate " + proxZoneId);
		seenProximates.add(id);
	    }
	}

	// Verify that no zone is recorded as proximate to itself
	for(Zone zone: cluster.getZones()) {
	    for(Integer proxZoneId: zone.getProximityList()) {
		if (proxZoneId == zone.getId())
		    throw new Exception("Zone " + zone.getId() + " is proximate to itself");
	    }
	}

	// Verify that each zone is proximate to all the other zones
	// and only to listed zones
	HashSet<Integer> seenZones = new HashSet<Integer>();
	for(Zone zone: cluster.getZones())
	    seenZones.add(new Integer(zone.getId()));
	for(Zone zone: cluster.getZones()) {
	    HashSet<Integer> expectProxes = (HashSet<Integer>) seenZones.clone();
	    expectProxes.remove(new Integer(zone.getId()));
	    for(Integer proxZoneId: zone.getProximityList()) {
		Integer id = new Integer(proxZoneId);
		if (!expectProxes.contains(id))
		    throw new Exception("Zone " + zone.getId() + " has unknown proximate zone " + proxZoneId);
		expectProxes.remove(id);
	    }
	    if (expectProxes.size() > 0) {
		StringBuffer buf = new StringBuffer();
		for (Integer id: expectProxes)
		    buf.append(" ").append(id);
		throw new Exception("Zone " + zone.getId() + " is missing proximates to " + buf);
	    }
	}
	return this;
    }

    // List of (regexp, replacement) pairs for normalizing hostnames
    static final String normalizationRules[] = {
	// LI-specific
	"\\.prod$", ".prod.linkedin.com",
	"\\.corp$", ".corp.linkedin.com",
	"\\.stg$", ".stg.linkedin.com"
    };

    // Edit the cluster hostnames to ensure each is a FQDN
    public ClusterBuilder normalizeHostnames() {
	LinkedList<Node> newNodes = new LinkedList<Node>();
	int numChanged = 0;
	for (Node node: cluster.getNodes()) {
	    String newHostname = node.getHost();
	    for (int i = 0 ; i < normalizationRules.length ; i += 2) {
		newHostname = Pattern.compile(normalizationRules[i])
				    .matcher(newHostname)
				    .replaceFirst(normalizationRules[i+1]);
	    }
	    // Build a new node if the hostname needed changing
	    if (!newHostname.equals(node.getHost())) {
		node = new Node(node.getId(),
				newHostname,
				node.getHttpPort(),
				node.getSocketPort(),
				node.getAdminPort(),
				node.getZoneId(),
				node.getPartitionIds(),
				node.getRestPort());
		numChanged++;
	    }
	    newNodes.add(node);
	}

	if (numChanged > 0) {
	    cluster = new Cluster(cluster.getName(),
				  newNodes,
				  new LinkedList<Zone>(cluster.getZones()));
	    System.out.println("Normalized " + numChanged + " hostnames");
	}
	return this;
    }

    // Edit the cluster to add a named zone, keeping the
    // proximity lists up to date.
    public ClusterBuilder addZone(NamedZone nz) throws Exception {
	ArrayList<Zone> newZones = new ArrayList<Zone>();
	// prox list for the new zone
	LinkedList<Integer> nzProx = new LinkedList<Integer>();
	// copy across the existing zones, with a new proximity each
	for (Zone zone: cluster.getZones()) {
	    if (zone.getId() == nz.id)
		throw new Exception("addZone: zone " + nz.id + " already present");
	    LinkedList<Integer> newProx = new LinkedList<Integer>();
	    for (Integer prox: zone.getProximityList())
		newProx.add(prox);
	    newProx.add(new Integer(nz.id));
	    newZones.add(new Zone(zone.getId(), newProx));
	    nzProx.add(new Integer(zone.getId()));
	}
	newZones.add(new Zone(nz.id, nzProx));
	cluster = new Cluster(cluster.getName(),
			      new LinkedList<Node>(cluster.getNodes()),
			      newZones);
	return this;
    }

    public ClusterBuilder addHosts(NamedZone nz, ArrayList<String> hosts) throws Exception {
	LinkedList<Node> newNodes = new LinkedList<Node>();
	int maxId = -1;
	int httpPort = -1;
	int adminPort = -1;
	int socketPort = -1;
	int restPort = -1;
	for (Node node: cluster.getNodes()) {
	    if (node.getId() > maxId)
		maxId = node.getId();
	    if (httpPort < 0)
		httpPort = node.getHttpPort();
	    if (adminPort < 0)
		adminPort = node.getAdminPort();
	    if (socketPort < 0)
		socketPort = node.getSocketPort();
	    if (restPort < 0)
		restPort = node.getRestPort();
	    newNodes.add(node);
	}
	// add new empty hosts
	for (String host: hosts) {
	    String x = host.substring(0, host.indexOf('-'));
	    if (!x.equals(nz.prefix))
		throw new Exception("Hostname " + host + " does not begin with prefix " + nz.prefix);
	    newNodes.add(new Node(++maxId,
				  host,
				  httpPort,
				  socketPort,
				  adminPort,
				  nz.id,
				  new LinkedList<Integer>(),   // no partitions
				  restPort));
	}
	cluster = new Cluster(cluster.getName(),
			      newNodes,
			      new LinkedList<Zone>(cluster.getZones()));
	return this;
    }

    public ClusterBuilder read(Reader reader) throws IOException {
	cluster = mapper.readCluster(reader);
	return this;
    }

    public ClusterBuilder write(Writer writer) throws IOException {
	writer.write(mapper.writeCluster(cluster));
	writer.flush();
	return this;
    }

    public Cluster getCluster() {
	return cluster;
    }
    public ClusterBuilder setCluster(Cluster cl) {
	cluster = cl;
	return this;
    }
}

class StoresBuilder {
    private StoreDefinitionsMapper mapper;
    private List<StoreDefinition> stores;

    StoresBuilder() {
	mapper = new StoreDefinitionsMapper();
	stores = new LinkedList<StoreDefinition>();
    }

    public StoresBuilder dump() {
	System.out.println("Stores {");

	for (StoreDefinition store: stores) {
	    System.out.println("    Store " + store.getName());
	    System.out.println("         replication factor " + store.getReplicationFactor());
	    System.out.println("         zone replication factors");
	    for (Map.Entry<Integer, Integer> pair: store.getZoneReplicationFactor().entrySet()) {
		System.out.println("             zone " + pair.getKey() + " factor " + pair.getValue());
	    }
	    System.out.println("");
	}

	System.out.println("} Stores");
	return this;
    }

    public StoresBuilder addZone(NamedZone nz) throws Exception {

	LinkedList<StoreDefinition> newStores = new LinkedList<StoreDefinition>();
	for (StoreDefinition store: stores) {
	    int totalRep = 0;
	    HashMap<Integer, Integer> newZoneReps = new HashMap<Integer, Integer>();
	    int repFactor = -1;
	    boolean haveRepFactor = false;
	    for (Map.Entry<Integer, Integer> pair: store.getZoneReplicationFactor().entrySet()) {
		if (!haveRepFactor) {
		    haveRepFactor = true;
		    repFactor = pair.getValue();
		}
		else if (repFactor != pair.getValue()) {
		    throw new Exception("Store " + store.getName() + ": not all zones have equal replication factors");
		}
		newZoneReps.put(pair.getKey(), repFactor);
		totalRep += repFactor;
	    }
	    if (!haveRepFactor)
		throw new Exception("Store " + store.getName() + ": no replication factors");
	    newZoneReps.put(nz.id, repFactor);
	    totalRep += repFactor;

	    newStores.add(new StoreDefinitionBuilder(store)
			    .setReplicationFactor(totalRep)
			    .setZoneReplicationFactor(newZoneReps)
			    .build());
	}

	stores = newStores;
	return this;
    }

    public StoresBuilder read(Reader reader) throws IOException {
	stores = mapper.readStoreList(reader);
	return this;
    }

    public StoresBuilder write(Writer writer) throws IOException {
	writer.write(mapper.writeStoreList(stores));
	writer.flush();
	return this;
    }

    public List<StoreDefinition> getStoreList() {
	return stores;
    }
}

class TheBlackAdder {

    // chosen out of a hat
    private static final int NUM_ATTEMPTS = 32;

    // LI-specific
    public static ArrayList<String> expandRange(String range) throws Exception {
	ProcessBuilder builder = new ProcessBuilder("eh", "-e", range);
	Process process = builder.start();
	InputStream stream = process.getInputStream();
	int c;
	StringBuffer buf = new StringBuffer(1024);
	while ((c = stream.read()) >= 0)
	    buf.append((char)c);
	StringTokenizer tok = new StringTokenizer(buf.toString(), "\n");
	ArrayList<String> list = new ArrayList<String>();
	while (tok.hasMoreTokens())
	    list.add(tok.nextToken());
	return list;
    }

    // LI-specific
    public static String makeRange(NamedZone nz, String cluster) {
	return "%" + nz.name + ".tag_hosts:voldemort." + cluster;
    }

    // LI-specific
    public static String makeBootstrapURL(NamedZone nz, String cluster) throws Exception {
	return "tcp://" + expandRange(makeRange(nz, cluster)).get(0) + ":10103";
    }

    public static String getMetadata(AdminClient adminClient, String key) throws Exception {
	int nodeId = 0;
	for (Node node: adminClient.getAdminClientCluster().getNodes()) {
	    nodeId = node.getId();
	    break;
	}
	Versioned<String> versioned = null;
	versioned = adminClient.metadataMgmtOps.getRemoteMetadata(nodeId, key);
	if(versioned == null)
	    return null;
	return versioned.getValue();
    }

    public static OptionParser makeOptionParser() {
        OptionParser parser = new OptionParser();

        parser.accepts("help", "print help information");

        parser.accepts("cluster", "cluster name")
              .withRequiredArg()
              .describedAs("cluster")
              .ofType(String.class);

        parser.accepts("zone", "zone name or id")
              .withRequiredArg()
              .describedAs("zone")
              .ofType(String.class);

        parser.accepts("testmode", "local test mode");

        return parser;
    }

    public static void main(String argv[]) throws Exception {
	OptionParser parser = makeOptionParser();
        OptionSet options = parser.parse(argv);

        if(options.has("help")) {
	    parser.printHelpOn(System.out);
            System.exit(0);
        }

	// Parse the --cluster option to a String
	String cluster = (String) options.valueOf("cluster");
	if(cluster == null) {
	    System.err.println("Missing required option --cluster");
	    parser.printHelpOn(System.err);
            System.exit(1);
	}

	// Parse the --zone option to a NamedZone
	String zoneName = (String) options.valueOf("zone");
	if(zoneName == null) {
	    System.err.println("Missing required option --zone");
	    parser.printHelpOn(System.err);
            System.exit(1);
	}
	NamedZone nz;
	if (Pattern.matches("^\\d+$", zoneName)) {
	    // specified a zone id
	    nz = NamedZone.getById(Integer.parseInt(zoneName));
	}
	else {
	    nz = NamedZone.getByName(zoneName);
	}
	if (nz == null) {
	    System.err.println("Unknown zone \"" + zoneName + "\"");
	    parser.printHelpOn(System.err);
            System.exit(1);
	}

	// Read the current cluster.xml
	Reader clusterXMLReader;
	Reader storesXMLReader;
	String workingDir;
	if(options.has("testmode")) {
	    workingDir = cluster;
	    clusterXMLReader = new FileReader(workingDir + "/current-cluster.xml");
	    storesXMLReader = new FileReader(workingDir + "/current-stores.xml");
	}
	else {
	    workingDir = "working.d";	    // TODO make a temporary directory
	    {
		// destroy and rebuild the working directory
		File wd = new File(workingDir);
		FileUtils.deleteDirectory(wd);
		wd.mkdirs();
	    }

	    String bootstrapURL = makeBootstrapURL(nz, cluster);
	    System.out.println("Bootstrap URL is " + bootstrapURL);
	    AdminClient adminClient = new AdminClient(bootstrapURL,
						      new AdminClientConfig(),
						      new ClientConfig());

	    String clusterXML = getMetadata(adminClient, "cluster.xml");
	    System.out.println("==== cluster.xml\n" + clusterXML);
	    Writer w = new FileWriter(workingDir + "/current-cluster.xml");
	    w.write(clusterXML);
	    w.close();
	    clusterXMLReader = new StringReader(clusterXML);

	    String storesXML = getMetadata(adminClient, "stores.xml");
	    System.out.println("==== stores.xml\n" + storesXML);
	    w = new FileWriter(workingDir + "/current-stores.xml");
	    w.write(clusterXML);
	    w.close();
	    storesXMLReader = new StringReader(storesXML);
	}

	// Read current-cluster.xml
	ClusterBuilder cbuilder = new ClusterBuilder();
	Cluster currentCluster = cbuilder
	    .read(clusterXMLReader)
	    .verify()
	    .dump()
	    .getCluster();

	// Generate interim-cluster.xml
	Cluster interimCluster = cbuilder
	    .addZone(nz)
	    .addHosts(nz, expandRange(makeRange(nz, cluster)))
	    .verify()
	    .dump()
	    .write(new FileWriter(workingDir + "/interim-cluster.xml"))
	    .getCluster();

	// Read current-stores.xml
	StoresBuilder sbuilder = new StoresBuilder();
	List<StoreDefinition> currentStores = sbuilder
	    .read(storesXMLReader)
//	    .verify()
	    .dump()
	    .getStoreList();

	// Generate final-stores.xml
	List<StoreDefinition> finalStores = sbuilder
	    .addZone(nz)
//	    .verify()
	    .dump()
	    .write(new FileWriter(workingDir + "/final-stores.xml"))
	    .getStoreList();

	// Generate final-cluster.xml
	Cluster finalCluster = Repartitioner.repartition(currentCluster,
							 currentStores,
							 interimCluster,
							 finalStores,
							 /*outputDir*/null,
							 NUM_ATTEMPTS,
							 /*disableNodeBalancing*/false,
							 /*disableZoneBalancing*/false,
							 /*enableRandomSwaps*/false,
							 Repartitioner.DEFAULT_RANDOM_SWAP_ATTEMPTS,
							 Repartitioner.DEFAULT_RANDOM_SWAP_SUCCESSES,
							 Repartitioner.DEFAULT_RANDOM_SWAP_ZONE_IDS,
							 /*enableGreedySwaps*/false,
							 Repartitioner.DEFAULT_GREEDY_SWAP_ATTEMPTS,
							 Repartitioner.DEFAULT_GREEDY_MAX_PARTITIONS_PER_NODE,
							 Repartitioner.DEFAULT_GREEDY_MAX_PARTITIONS_PER_ZONE,
							 Repartitioner.DEFAULT_GREEDY_SWAP_ZONE_IDS,
							 Repartitioner.DEFAULT_MAX_CONTIGUOUS_PARTITIONS);
	// Note that we need to normalize the hostname *after* calling
	// the Repartitioner because it tests that the nodes' hostnames
	// are the same between the current and the interim clusters.
	cbuilder
	    .setCluster(finalCluster)
	    .normalizeHostnames()
	    .write(new FileWriter(workingDir + "/final-cluster.xml"));
    }
}
