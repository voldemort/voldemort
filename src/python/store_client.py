import voldemort_client_pb2 as protocol
from xml.dom import minidom
import socket
import struct
import time

# A Voldemort client. Each client uses a single connection to one Voldemort server. All routing is done server-side.
# TODO:
#  1. Failover across nodes in the cluster
#  2. Throw real exceptions instead of strings
#  3. cluster xml parsing
#  4. Version reconciliation
#  5. Re-connect to a random server every n requests to avoid bunching up requests on the same servers
#  6. Reduce calls to send()?
#  7. Serialization in the client?

class node:
	"""A Voldemort node with the appropriate host and port information for contacting that node"""
	
	def __init__(self, id, host, socket_port, http_port, admin_port, partitions, is_available = True, last_contact = None):
		self.id = id
		self.host = host
		self.socket_port = socket_port
		self.http_port = http_port
		self.admin_port = admin_port
		self.partitions = partitions
		self.is_available = True
		if not last_contact:
		  self.last_contact = time.clock()
	
	def __repr__(self):
		return 'node(id = ' + str(id) + ', host = ' + host + ', socket_port = ' + str(socket_port) + \
		       ', http_port = ' + str(http_port) + ', admin_port = ' + str(admin_port) + \
		       ', partitions = ' + paritions + ')'

def parse_cluster(xml):
	"""Parse the cluster.xml file and return a dictionary of the nodes in the cluster indexed by node id """
	pass

class store_client:

  def __init__(self, store_name, host, port, node_id):
    self.node_id = node_id
    self.store_name = store_name
    self.connection = socket.socket()
    self.connection.connect((host, port))
    self.open = True

  def _send_req(self, buff):
    """Send a size-delimited value"""
    assert self.open, 'Store has been closed.'
    self.connection.send(struct.pack('>i', len(buff)))
    self.connection.send(buff)
  
  def _receive_resp(self):
    """Read a size-delimited value"""
    assert self.open, 'Store has been closed.'
    size = struct.unpack('>i', self.connection.recv(4))
    print 'response size', size[0]
    return self.connection.recv(size[0])
    
  def _create_request(self):
    req = protocol.VoldemortRequest()
    req.should_route = True
    req.store = self.store_name
    return req
    
  def _check_error(self, resp):
    if resp._has_error:
      raise 'Received error code ' + str(resp.error.error_code) + ': ' + resp.error.error_message
      
  def _inc_version__(self, versioned):
    print 'no version increment implemented'
    pass
    
  def get(self, key):
    """Execute get request. Returns a (value, version) pair."""
    
    req = self._create_request()
    req.type = protocol.GET
    req.get.key = key
    
    # send request
    self._send_req(req.SerializeToString())

    # read and parse response
    resp_str = self._receive_resp()
    print resp_str
    resp = protocol.GetResponse.ParseFromString(resp_str)
    self._check_error(resp)
    return (resp.versioned.value, resp.versioned.version)
    
    
  def get_all(self, keys):
    """Execute get request for multiple keys. Returns a list of (value, version) pairs."""
    
    req = self._create_request()
    req.type = protocol.GET_ALL
    get_req = protocol.GetAllRequest()
    get_req.keys = keys
    req.get_all = get_req
    
    # send request
    self._send_req(req.SerializeToString())

    # read and parse response
    resp_str = self._receive_resp()
    resp = protocol.GetAllResponse.ParseFromString(resp_str)
    self._check_error(resp)
    values = {}
    for key_val in resp.values:
      values[key_val.key] = key_val.values
    return values
      
    
  def put(self, key, value, version):
    """Execute put request."""
    
    req = self._create_request()
    req.type = protocol.PUT
    put_req = protocol.PutRequest()
    put_req.key = key
    put_req.versioned = versioned
    req.put = put_req
    
    # send request
    self._send_req(req.SerializeToString())

    # read and parse response
    resp_str = self._receive_resp()
    resp = protocol.PutResponse.ParseFromString(resp_str)
    self._check_error(resp)
    self._inc_version__(versioned)
    return versioned.version
    
    
  def delete(self, key, version):
    """Execute a delete request."""
    
    req = self._create_request()
    req.type = protocol.DELETE
    delete_req = protocol.DeleteRequest()
    delete_req.key = key
    delete_req.version = version
    delete.delete = delete_req
    
    # send request
    self._send_req(req.SerializeToString())

    # read and parse response
    resp_str = self._receive_resp()
    resp = protocol.DeleteResponse.ParseFromString(resp_str)
    self._check_error(resp)
    
    
  def close(self):
    """Close the connection this store maintains."""
    self.open = False
    self.connection.close()

s = store_client('test', 'localhost', 6666, 0)
s.get('hello')
    