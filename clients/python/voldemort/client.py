import socket
import struct
import time
import sys
import re
import random
import logging

import protocol.voldemort_client_pb2 as protocol
from xml.dom import minidom
from datetime import datetime

import serialization

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
def _child(elmt, name, required=True):
    children = [child for child in elmt.childNodes
                if child.nodeType == minidom.Node.ELEMENT_NODE and child.tagName == name]
    if not children:
        if required:
            raise VoldemortException("No child '%s' for element '%s'." % (name, elmt.nodeName))
        else:
            return None

    if len(children) > 1:
        raise VoldemortException("Multiple children '%s' for element '%s'." % (name, elmt.nodeName))
    return children[0]


## Get the child text of a single element
def _child_text(elmt, name, required=True, default=None):
    if default:
        required = False

    child = _child(elmt, name, required=required)
    if not child:
        return default

    return _extract_text(child)


def _int_or_none(s):
    if s is None:
        return s
    return int(s)


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


class Store:
    def __init__(self, store_node):
        self.name = _child_text(store_node, "name")
        self.persistence = _child_text(store_node, "persistence")
        self.routing = _child_text(store_node, "routing")
        self.routing_strategy = _child_text(store_node, "routing-strategy", default="consistent-routing")
        self.replication_factor = int(_child_text(store_node, "replication-factor"))
        self.required_reads = int(_child_text(store_node, "required-reads"))
        self.preferred_reads = _int_or_none(_child_text(store_node, "preferred-reads", required=False))
        self.required_writes = int(_child_text(store_node, "required-writes"))
        self.preferred_writes = _int_or_none(_child_text(store_node, "preferred-writes", required=False))

        key_serializer_node = _child(store_node, "key-serializer")
        try:
            self.key_serializer_type = _child_text(key_serializer_node, "type")
            self.key_serializer = self._create_serializer(self.key_serializer_type, key_serializer_node)
        except serialization.SerializationException, e:
            logging.warn("Error while creating key serializer for store [%s]: %s" % (self.name, e))
            self.key_serializer_type = "invalid"
            self.key_serializer = serialization.UnimplementedSerializer("invalid")

        value_serializer_node = _child(store_node, "value-serializer")
        try:
            self.value_serializer_type = _child_text(value_serializer_node, "type")
            self.value_serializer = self._create_serializer(self.value_serializer_type, value_serializer_node)
        except serialization.SerializationException, e:
            logging.warn("Error while creating value serializer for store [%s]: %s" % (self.name, e))
            self.value_serializer_type = "invalid"
            self.value_serializer = serialization.UnimplementedSerializer("invalid")

    def _create_serializer(self, serializer_type, serializer_node):
        if serializer_type not in serialization.SERIALIZER_CLASSES:
            return serialization.UnimplementedSerializer(serializer_type)

        return serialization.SERIALIZER_CLASSES[serializer_type].create_from_xml(serializer_node)

    @staticmethod
    def parse_stores_xml(xml, store_name):
        doc = minidom.parseString(xml)
        store_nodes = doc.getElementsByTagName("store")
        for store_node in store_nodes:
            name = _child_text(store_node, "name")
            if name == store_name:
                return Store(store_node)

        return None


class VoldemortException(Exception):
    def __init__(self, msg, code = 1):
        self.code = code
        self.msg = msg

    def __str__(self):
        return repr(self.msg)


class StoreClient:
    """A simple Voldemort client. It is single-threaded and supports only server-side routing."""

    def __init__(self, store_name, bootstrap_urls, reconnect_interval = 500, conflict_resolver = None):
        self.store_name = store_name
        self.request_count = 0
        self.conflict_resolver = conflict_resolver
        self.nodes, self.store = self._bootstrap_metadata(bootstrap_urls, store_name)
        if not self.store:
            raise VoldemortException("Cannot find store [%s] at %s" % (store_name, bootstrap_urls))

        self.node_id = random.randint(0, len(self.nodes) - 1)
        self.node_id, self.connection = self._reconnect()
        self.reconnect_interval = reconnect_interval
        self.open = True
        self.key_serializer = self.store.key_serializer
        self.value_serializer = self.store.value_serializer

    def _make_connection(self, host, port):
        protocol = 'pb0'
        logging.debug('Attempting to connect to ' + host + ':' + str(port))
        connection = socket.socket()
        connection.connect((host, port))
        logging.debug('Connection succeeded, negotiating protocol')
        connection.send(protocol)
        resp = connection.recv(2)
        if resp != 'ok':
            raise VoldemortException('Server does not understand the protocol ' + protocol)
        logging.debug('Protocol negotiation suceeded')
        return connection


    ## Connect to a the next available node in the cluster
    ## returns a tuple of (node_id, connection)
    def _reconnect(self):
        num_nodes = len(self.nodes)
        attempts = 0
        new_node_id = self.node_id
        while attempts < num_nodes:
            new_node_id = (new_node_id + 1) % num_nodes
            new_node = self.nodes[new_node_id]
            connection = None
            try:
                connection = self._make_connection(new_node.host, new_node.socket_port)
                self.request_count = 0
                return new_node_id, connection
            except socket.error, (err_num, message):
                self._close_socket(connection)
                logging.warn('Error connecting to node ' + str(new_node_id) + ': ' + message)
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
        size = struct.unpack('>i', size_bytes)[0]

        bytes_read = 0
        data = []

        while size and bytes_read < size:
            chunk = connection.recv(size - bytes_read)
            bytes_read += len(chunk)
            data.append(chunk)

        return ''.join(data)


    ## Bootstrap cluster metadata from a list of urls of nodes in the cluster.
    ## The urls are tuples in the form (host, port).
    ## A dictionary of node_id => node is returned.
    def _bootstrap_metadata(self, bootstrap_urls, store_name):
        random.shuffle(bootstrap_urls)
        for host, port in bootstrap_urls:
            logging.debug('Attempting to bootstrap metadata from ' + host + ':' + str(port))
            connection = None
            try:
                connection = self._make_connection(host, port)
                cluster_xmls = self._get_with_connection(connection, 'metadata', 'cluster.xml', should_route = False)
                if len(cluster_xmls) != 1:
                    raise VoldemortException('Expected exactly one version of the metadata but found ' + str(cluster_xmls))
                nodes = Node.parse_cluster(cluster_xmls[0][0])
                logging.debug('Bootstrap from ' + host + ':' + str(port) + ' succeeded, found ' + str(len(nodes)) + " nodes.")
                stores_xml = self._get_with_connection(connection, 'metadata', 'stores.xml', should_route=False)[0][0]
                store = Store.parse_stores_xml(stores_xml, store_name)

                return nodes, store
            except socket.error, (err_num, message):
                logging.warn('Metadata bootstrap from ' + host + ':' + str(port) + " failed: " + message)
            finally:
                self._close_socket(connection)
        raise VoldemortException('All bootstrap attempts failed')


    ## check if the server response has an error, if so throw an exception
    def _check_error(self, resp):
        if resp.error and resp.error.error_code != 0:
            raise VoldemortException(resp.error.error_message, resp.error.error_code)


    ## Increment the version for a vector clock
    def _increment(self, clock):
        new_clock = protocol.VectorClock()
        new_clock.MergeFrom(clock)

        # See if we already have a version for this guy, if so increment it
        for entry in new_clock.entries:
            if entry.node_id == self.node_id:
                entry.version += 1
                return new_clock

        # Otherwise add a version
        entry = new_clock.entries.add()
        entry.node_id = self.node_id
        entry.version = 1
        new_clock.timestamp = int(time.time() * 1000)

        return new_clock

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
            except socket.error, (err_num, message):
                logging.warn('Error while performing ' + fun.__name__ + ' on node ' + str(self.node_id) + ': ' + message)
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

        raw_key = self.key_serializer.writes(key)
        return [(self.value_serializer.reads(value), version)
                for value, version in self._execute_request(self._get, [raw_key])]


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

        raw_keys = [self.key_serializer.writes(key) for key in keys]
        return dict((self.key_serializer.reads(key), [(self.value_serializer.reads(value), version)
                                                      for value, version in versioned_values])
                    for key, versioned_values in self._execute_request(self._get_all, [raw_keys]).iteritems())


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
        return self._increment(version)


    def put(self, key, value, version = None):
        """Execute a put request using the given key and value. If no version is specified a get(key) request
           will be done to get the current version. The updated version is returned."""

        raw_key = self.key_serializer.writes(key)
        raw_value = self.value_serializer.writes(value)

        # if we don't have a version, fetch one
        if not version:
            version = self._fetch_version(key)
        return self._execute_request(self._put, [raw_key, raw_value, version])


    def maybe_put(self, key, value, version = None):
        """Execute a put request using the given key and value. If the version being put is obsolete,
           no modification will be made and this function will return None. Otherwise it will return the new version."""
        try:
            return self.put(key, value, version)
        except:
            return None

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

        raw_key = self.key_serializer.writes(key)

        # if we don't have a version, fetch one
        if version == None:
            version = self._fetch_version(key)
        return self._execute_request(self._delete, [raw_key, version])


    def close(self):
        """Close the connection this store maintains."""
        self.open = False
        self.connection.close()




