import socket
import struct
import time
import sys
import re
import random
import logging
import sets

import voldemort_client_pb2 as protocol
from xml.dom import minidom
from datetime import datetime

##################################################################
# A Voldemort client. Each client uses a single connection to one
# Voldemort server. All routing is done server-side.
##################################################################


## Extract all the child text of the given element
def _extract_text(elm):
	if elm.nodeType == minidom.Node.TEXT_NODE:
		return elm.data
	elif elm.nodeType == minidom.Node.ELEMENT_NODE:
		text = ""
		for child in elm.childNodes:
			text += _extract_text(child)
		return text
		

## Get a single child from the element, if there are multiple children, explode.
def _child(elmt, name):
	children = elmt.getElementsByTagName(name)
	if len(children) != 1:
		raise Exception, "No child '" + str(name) + "' for element " + str(elmt.nodeName)
	return children[0]
	
	
## Get the child text of a single element
def _child_text(elmt, name):
	return _extract_text(_child(elmt, name))



##################################################################
# A node class representing a single voldemort server in the
# cluster. The cluster itself is just a list of nodes
##################################################################
class Node:
	"""A Voldemort node with the appropriate host and port information for contacting that node"""
	
	def __init__(self, id, host, socket_port, http_port, partitions, is_available = True, last_contact = None):
		self.id = id
		self.host = host
		self.socket_port = socket_port
		self.http_port = http_port
		self.partitions = partitions
		self.is_available = True
		if not last_contact:
		  self.last_contact = time.clock()
	
	def __repr__(self):
		return 'node(id = ' + str(self.id) + ', host = ' + self.host + ', socket_port = ' + str(self.socket_port) + \
		       ', http_port = ' + str(self.http_port) + ', partitions = ' + ', '.join(map(str, self.partitions)) + ')'

	@staticmethod
	def parse_cluster(xml):
		"""Parse the cluster.xml file and return a dictionary of the nodes in the cluster indexed by node id """
		doc = minidom.parseString(xml)
		nodes = {}
		for curr in doc.getElementsByTagName('server'):
			id = int(_child_text(curr, 'id'))
			host = _child_text(curr, 'host')
			http_port = int(_child_text(curr, 'http-port'))
			socket_port = int(_child_text(curr, 'socket-port'))
			partition_str = _child_text(curr, 'partitions')
			partitions = [int(p) for p in re.split('[\s,]+', partition_str)]
			nodes[id] = Node(id = id, host = host, socket_port = socket_port, http_port = http_port, partitions = partitions)
		return nodes



class VoldemortException(Exception):
	def __init__(self, message, code = 1):
		self.code = code
		self.message = message
		
	def __str__(self):
		return repr(self.message)
	


class StoreClient:
	"""A simple Voldemort client. It is single-threaded and supports only server-side routing."""

	def __init__(self, store_name, bootstrap_urls, reconnect_interval = 500, conflict_resolver = None):
		self.store_name = store_name
		self.request_count = 0
		self.conflict_resolver = conflict_resolver
		self.nodes = self._bootstrap_metadata(bootstrap_urls)
		self.node_id = random.randint(0, len(self.nodes) - 1)
		self.node_id, self.connection = self._reconnect()
		self.reconnect_interval = reconnect_interval
		self.open = True
		
	
	## Connect to a the next available node in the cluster
	## returns a tuple of (node_id, connection)
	def _reconnect(self):
		num_nodes = len(self.nodes)
		attempts = 0
		while attempts < num_nodes:
			new_node_id = self.node_id + 1 % num_nodes
			new_node = self.nodes[new_node_id]
			connection = None
			try:
				logging.debug('Attempting to connect to node ' + str(new_node_id))
				connection = socket.socket()
				connection.connect((new_node.host, new_node.socket_port))
				logging.debug('Connection succeeded')
				self.request_count = 0
				return new_node_id, connection
			except socket.error, exp:
				self._close_socket(connection)
				logging.warn('Error connecting to node ' + str(new_node_id) + ': ' + str(exp))
				attempts += 1
				
		# If we get here all nodes have failed us, explode
		raise VoldemortException('Connections to all nodes failed.')
		
	
	## Safely close the socket, catching and logging any exceptions
	def _close_socket(self, socket):
		try:
			if socket:
				socket.close()
		except socket.error, exp:
			logging.error('Error while closing socket: ' + str(exp))
	
	
	## Check if the the number of requests made on this connection is greater than the reconnect interval.
	## If so reconnect to a random node in the cluster. No attempt is made at preventing the reconnecting
	## from going back to the same node
	def _maybe_reconnect(self):
		if self.request_count >= self.reconnect_interval:
			logging.debug('Completed ' + str(self.request_count) + ' requests using this connection, reconnecting...')
			self._close_socket(self.connection)
			self.node_id, self.connection = self._reconnect()
			
	
	## send a request to the server using the given connection
	def _send_request(self, connection, req_bytes):
		connection.send(struct.pack('>i', len(req_bytes)) + req_bytes)
		self.request_count += 1


	## read a response from the connection
	def _receive_response(self, connection):
		size_bytes = connection.recv(4)
		size = struct.unpack('>i', size_bytes)
		return connection.recv(size[0])
			
	
	## Bootstrap cluster metadata from a list of urls of nodes in the cluster. 
	## The urls are tuples in the form (host, port).
	## A dictionary of node_id => node is returned.
	def _bootstrap_metadata(self, bootstrap_urls):
		random.shuffle(bootstrap_urls)
		for host, port in bootstrap_urls:
			logging.debug('Attempting to bootstrap metadata from ' + host + ':' + str(port))
			connection = socket.socket()
			connection.connect((host, port))
			try:
				cluster_xmls = self._get_with_connection(connection, 'metadata', 'cluster.xml', should_route = False)
				if len(cluster_xmls) != 1:
					raise Exception, 'Expected exactly one version of the metadata but found ' + str(cluster_xmls)
				logging.debug('Bootstrap succeeded')
				return Node.parse_cluster(cluster_xmls[0][0])
			except Exception, exp:
				logging.warn('Metadata bootstrap from ' + host + ':' + str(port) + " failed: " + str(exp))
			self._close_socket(connection)
		raise VoldemortException('All bootstrap attempts failed')
		
    
    ## check if the server response has an error, if so throw an exception
	def _check_error(self, resp):
		if resp._has_error:
			raise VoldemortException(resp.error.error_message, resp.error.error_code)
			
    
	## Increment the version for a vector clock
	def _increment(self, clock):
		# See if we already have a version for this guy, if so increment it
		for entry in clock.entries:
			if entry.node_id == self.node_id:
				entry.version += 1
				return
		# Otherwise add a version
		entry = clock.entries.add()
		entry.node_id = self.node_id
		entry.version = 1
		clock.timestamp = int(time.time() * 1000)
		
		
	## Take a list of versions, and, if a conflict resolver has been given, resolve any conflicts that can be resolved
	def _resolve_conflicts(self, versions):
		if self.conflict_resolver and versions:
			return self.conflict_resolver(versions)
		else:
			return versions
				
	
	## Turn a protocol buffer list of versioned items into a python list of items
	def _extract_versions(self, pb_versioneds):
		versions = []
		for versioned in pb_versioneds:
			versions.append((versioned.value, versioned.version))
		return self._resolve_conflicts(versions)
	
	## A basic request wrapper, that handles reconnection logic and failures
	def _execute_request(self, fun, args):
		assert self.open, 'Store has been closed.'
		self._maybe_reconnect()
		
		failures = 0
		num_nodes = len(self.nodes)
		while failures < num_nodes:
			try:
				return apply(fun, args)
			except socket.error, exp:
				logging.warn('Error while performing ' + fun.__name__ + ' on node ' + str(self.node_id) + ': ' + str(exp))
				self._reconnect()
				failures += 1
		raise VoldemortException('All nodes are down, ' + fun.__name__ + ' failed.')
		
	
	## An internal get function that take the connection and store name as parameters. This is
	## used by both the public get() method and also the metadata bootstrap process
	def _get_with_connection(self, connection, store_name, key, should_route):
		"""Execute get request to the given store. Returns a (value, version) pair."""

		req = protocol.VoldemortRequest()
		req.should_route = should_route
		req.store = store_name
		req.type = protocol.GET
		req.get.key = key
		
		# send request
		self._send_request(connection, req.SerializeToString())

		# read and parse response
		resp_str = self._receive_response(connection)
		resp = protocol.GetResponse()
		resp.ParseFromString(resp_str)
		self._check_error(resp)

		return self._extract_versions(resp.versioned)
	
	
	## Inner helper function for get
	def _get(self, key):
		return self._get_with_connection(self.connection, self.store_name, key, True)
	
			
	def get(self, key):
		"""Execute a get request. Returns a list of (value, version) pairs."""
		return self._execute_request(self._get, [key])
		
	
	## Inner get_all method that takes the connection and store_name as parameters
	def _get_all(self, keys):
		req = protocol.VoldemortRequest()
		req.should_route = True
		req.store = self.store_name
		req.type = protocol.GET_ALL
		for key in keys:
			req.getAll.keys.append(key)

		# send request
		self._send_request(self.connection, req.SerializeToString())

		# read and parse response
		resp_str = self._receive_response(self.connection)
		resp = protocol.GetAllResponse()
		resp.ParseFromString(resp_str)
		self._check_error(resp)
		values = {}
		for key_val in resp.values:
			values[key_val.key] = self._extract_versions(key_val.versions)
		return values
		

	def get_all(self, keys):
		"""Execute get request for multiple keys given as a list or tuple. 
		   Returns a dictionary of key => [(value, version), ...] pairs."""
		return self._execute_request(self._get_all, [keys])
		
	
	## Get the current version of the given key by doing a get request to the store
	def _fetch_version(self, key):
		versioned = self.get(key)
		if versioned:
			version = versioned[0][1]
		else:
			version = protocol.VectorClock()
			version.timestamp = int(time.time() * 1000)
		return version
      
    
	def _put(self, key, value, version):					
		req = protocol.VoldemortRequest()
		req.should_route = True
		req.store = self.store_name
		req.type = protocol.PUT
		req.put.key = key
		req.put.versioned.value = value
		req.put.versioned.version.MergeFrom(version)

		# send request
		self._send_request(self.connection, req.SerializeToString())

		# read and parse response
		resp_str = self._receive_response(self.connection)
		resp = protocol.PutResponse()
		resp.ParseFromString(resp_str)
		self._check_error(resp)
		self._increment(version)
		return version
		
	def put	(self, key, value, version = None):
		"""Execute a put request using the given key and value. If no version is specified a get(key) request
		   will be done to get the current version. The updated version is returned."""
		# if we don't have a version, fetch one
		if not version:
			version = self._fetch_version(key)
		return self._execute_request(self._put, [key, value, version])
    
    
	def _delete(self, key, version):
		req = protocol.VoldemortRequest()
		req.should_route = True
		req.store = self.store_name
		req.type = protocol.DELETE
		req.delete.key = key
		req.delete.version.MergeFrom(version)

		# send request
		self._send_request(self.connection, req.SerializeToString())

		# read and parse response
		resp_str = self._receive_response(self.connection)
		resp = protocol.DeleteResponse()
		resp.ParseFromString(resp_str)
		self._check_error(resp)
		
		return resp.success
    
		
	def delete(self, key, version = None):
		"""Execute a delete request, deleting all keys up to and including the given version. 
		   If no version is given a get(key) request will be done to find the latest version."""
		# if we don't have a version, fetch one
		if version == None:
			version = self._fetch_version(key)
		return self._execute_request(self._delete, [key, version])
    

	def close(self):
		"""Close the connection this store maintains."""
		self.open = False
		self.connection.close()

	
	
		
