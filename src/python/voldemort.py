import socket
import struct
import time
import sys
import re
import random
import logging

import voldemort_client_pb2 as protocol
from xml.dom import minidom
from datetime import datetime

##################################################################
# A Voldemort client. Each client uses a single connection to one
# Voldemort server. All routing is done server-side.
##################################################################

##################################################################
# TODO:
#  - Failover across nodes in the cluster
#  - Throw real exceptions instead of strings
#  - Version reconciliation
#  - Reduce calls to send()?
#  - Serialization in the client
##################################################################


##################################################################
# Some XML helper functions
##################################################################

def _extract_text(elm):
	"""Extract all the child text of the given element."""
	if elm.nodeType == minidom.Node.TEXT_NODE:
		return elm.data
	elif elm.nodeType == minidom.Node.ELEMENT_NODE:
		text = ""
		for child in elm.childNodes:
			text += _extract_text(child)
		return text

def _child(elmt, name):
	"""Get a single child from the element, if there are multiple children, explode."""
	children = elmt.getElementsByTagName(name)
	if len(children) != 1:
		raise "No child '" + str(name) + "' for element " + str(elmt.nodeName)
	return children[0]
	
def child_text(elmt, name):
	"""Get the child text of a single element"""
	return _extract_text(_child(elmt, name))


##################################################################
# A node class representing a single voldemort server in the
# cluster. The cluster itself is just a list of nodes
##################################################################
class node:
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
			id = int(child_text(curr, 'id'))
			host = child_text(curr, 'host')
			http_port = int(child_text(curr, 'http-port'))
			socket_port = int(child_text(curr, 'socket-port'))
			partition_str = child_text(curr, 'partitions')
			partitions = [int(p) for p in re.split('[\s,]+', partition_str)]
			nodes[id] = node(id = id, host = host, socket_port = socket_port, http_port = http_port, partitions = partitions)
		return nodes


class store_client:
	"""A simple Voldemort client. It is single-threaded and supports only server-side routing."""

	def __init__(self, store_name, bootstrap_urls, reconnect_interval = 500):
		self.store_name = store_name
		self.request_count = 0
		self.nodes = self._bootstrap_metadata(bootstrap_urls)
		self.node_id, self.connection = self._connect_to_random()
		self.reconnect_interval = reconnect_interval
		self.open = True
		
	
	## Connect to a random node in the cluster
	## returns a tuple of (node_id, connection)
	def _connect_to_random(self):
		node_id = random.randint(0, len(self.nodes) - 1)
		curr_node = self.nodes[node_id]
		connection = socket.socket()
		connection.connect((curr_node.host, curr_node.socket_port))
		return node_id, connection
		
	
	## Check if the the number of requests made on this connection is greater than the reconnect interval.
	## If so reconnect to a random node in the cluster. No attempt is made at preventing the reconnecting
	## from going back to the same node
	def _maybe_reconnect(self):
		if self.request_count >= self.reconnect_interval:
			self.connection.close()
			self.node_id, self.connection = self._connect_to_random()
			logging.debug('Closing connection to node ' + str(self.node_id) + ' after ' + str(self.request_count) + 
			              ' requests and reconnecting to node ' + str(self.node_id))
			self.request_count = 0
			
	
	## Bootstrap cluster metadata from a list of urls of nodes in the cluster. 
	## The urls are tuples in the form (host, port).
	## A dictionary of node_id => node is returned.
	def _bootstrap_metadata(self, bootstrap_urls):
		random.shuffle(bootstrap_urls)
		for host, port in bootstrap_urls:
			connection = socket.socket()
			connection.connect((host, port))
			try:
				cluster_xmls = self._get(connection, 'metadata', 'cluster.xml', should_route = False)
				if len(cluster_xmls) != 1:
					raise 'Expected exactly one version of the metadata but found ' + str(cluster_xmls)
				return node.parse_cluster(cluster_xmls[0][0])
			except ValueError:
				logging.warn('Metadata bootstrap from ' + host + ':' + str(port) + " failed")
			if connection:
				connection.close()
		raise 'All bootstrap attempts failed'
		

	## send a request to the server using the connection
	def _send_request(self, connection, req_bytes):
		connection.send(struct.pack('>i', len(req_bytes)))
		connection.send(req_bytes)
		self.request_count += 1
  
	## read a response from the connection
	def _receive_response(self, connection):
		"""Read a size-delimited value"""
		size_bytes = connection.recv(4)
		size = struct.unpack('>i', size_bytes)
		return connection.recv(size[0])
    
	## create a protocol buffers request object and populate a few common fields
	def _create_request(self, store_name, should_route=True):
		req = protocol.VoldemortRequest()
		req.should_route = should_route
		req.store = store_name
		return req
    
    ## check if the server response has an error, if so throw an exception
	def _check_error(self, resp):
		if resp._has_error:
			raise str('Received error code ' + str(resp.error.error_code) + ': ' + resp.error.error_message)
    
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
	
	## Get the current version of the given key by doing a get request to the store
	def _version_of(self, key):
		versioned = self.get(key)
		if versioned:
			version = versioned[0][1]
		else:
			version = protocol.VectorClock()
			version.timestamp = int(time.time() * 1000)
		return version
	
	## Turn a protocol buffer list of versioned items into a python list of items
	def _versions(self, pb_versioneds):
		versions = []
		for versioned in pb_versioneds:
			versions.append((versioned.value, versioned.version))
		return versions
	
	## An internal get function that take the connection and store name as parameters. This is
	## used by both the public get() method and also the metadata bootstrap process
	def _get(self, connection, store_name, key, should_route):
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
		return self._versions(resp.versioned)

	
	def get(self, key):
		"""Execute a get request. Returns a list of (value, version) pairs."""
		
		assert self.open, 'Store has been closed.'
		self._maybe_reconnect()
		
		return self._get(self.connection, self.store_name, key, should_route = True)

	def get_all(self, keys):
		"""Execute get request for multiple keys given as a list or tuple. 
		   Returns a dictionary of key => [(value, version), ...] pairs."""

		assert self.open, 'Store has been closed.'
		self._maybe_reconnect()

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
			values[key_val.key] = self._versions(key_val.versions)
		return values
      
    
	def put(self, key, value, version = None):
		"""Execute a put request using the given key and value. If no version is specified a get(key) request
		   will be done to get the current version. The updated version is returned."""
		
		assert self.open, 'Store has been closed.'
		self._maybe_reconnect()
		
		# if we don't have a version, fetch one
		if version == None:
			version = self._version_of(key)
			
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
    
    
	def delete(self, key, version = None):
		"""Execute a delete request, deleting all keys up to and including the given version. 
		   If no version is given a get(key) request will be done to find the latest version."""
		
		assert self.open, 'Store has been closed.'
		self._maybe_reconnect()
		
		# if we don't have a version, fetch one
		if version == None:
			version = self._version_of(key)

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
    
    
	def close(self):
		"""Close the connection this store maintains."""
		self.open = False
		self.connection.close()

if __name__ == '__main__':
	## some random tests
	s = store_client('test', [('localhost', 6666)])
	print 'put("hello3", "world") = ', s.put("hello3", "world")
	print
	print 'get("hello3") =', s.get('hello3')
	print
	print 'delete("hello3") =', s.delete('hello3')
	
	s.put("a1", "1")
	s.put("a2", "2")
	s.put("a3", "3")
	print "get_all(a1, a2, a3)", s.get_all(["a1", "a2", "a3"])
	
	start = time.time()
	requests = 100
	for i in xrange(requests):
		s.get('hello3')
	print requests/(time.time() - start), 'requests per second'
		
