require 'socket'
require 'timeout'

require 'protos/voldemort-client.pb'

class TCPConnection < Connection
  include Voldemort

  attr_accessor :socket

  SOCKET_TIMEOUT = 3

  def connect_to(host, port)
    begin
      timeout(SOCKET_TIMEOUT) do
        self.socket = TCPSocket.open(host, port)
        self.send_protocol_version
        if(protocol_handshake_ok?)
          return self.socket
        else
          raise "There was an error connecting to the node"
        end
      end
    rescue Timeout::Error
      raise "Timeout when connecting to node"
    rescue
      false
    end
  end

  def get_from(db_name, key, route = true)
    request = VoldemortRequest.new
    request.should_route = route
    request.store = db_name
    request.type = RequestType::GET
    request.get = GetRequest.new
    request.get.key = key

    self.send(request)          # send the request
    raw_response = self.receive # read the response
    response = GetResponse.new.parse_from_string(raw_response) # compose the get object based on the raw response
    reconnect_when_errors_in(response)
    response
  end

  def get_all_from(db_name, keys, route = true)
    request = VoldemortRequest.new
    request.should_route = route
    request.store = db_name
    request.type = RequestType::GET_ALL
    request.getAll = GetAllRequest.new
    request.getAll.keys = keys

    self.send(request)          # send the request
    raw_response = self.receive # read the response
    response = GetAllResponse.new.parse_from_string(raw_response) # compose the get object based on the raw response
    reconnect_when_errors_in(response)
    response
  end

  def put_from(db_name, key, value, version = nil, route = true)
    version = get_version(key) unless version
    request = VoldemortRequest.new
    request.should_route = route
    request.store = db_name
    request.type = RequestType::PUT
    request.put = PutRequest.new
    request.put.key = key
    request.put.versioned = Versioned.new
    request.put.versioned.value = value
    request.put.versioned.version = VectorClock.new
    request.put.versioned.version.merge_from(version)

    self.send(request)          # send the request
    raw_response = self.receive # read the response
    response = PutResponse.new.parse_from_string(raw_response)
    reconnect_when_errors_in(response)

    add_to_versions(version) # add version or increment when needed
    version
  end

  def delete_from(db_name, key, version = nil, route = true)
    version = get_version(key) unless version
    request = VoldemortRequest.new
    request.should_route = route
    request.store  = db_name
    request.type   = RequestType::DELETE
    request.delete = DeleteRequest.new
    request.delete.key = key
    request.delete.version = VectorClock.new
    request.delete.version.merge_from(version)

    self.send(request)          # send the request
    raw_response = self.receive # read the response
    response = DeleteResponse.new.parse_from_string(raw_response)
    reconnect_when_errors_in(response)
    response.success
  end

  def add_to_versions(version)
    entry = version.entries.detect { |e| e.node_id == self.connected_node.id.to_i }
    if(entry)
      entry.version += 1
    else
      entry = ClockEntry.new
      entry.node_id = self.connected_node.id.to_i
      entry.version = 1
      version.entries << entry
      version.timestamp = Time.new.to_i * 1000
    end
    version
  end

  def get_version(key)
    other_version = get(key)[1][0]
    if(other_version)
      return other_version.version
    else
      version = VectorClock.new
      version.timestamp = Time.new.to_i * 1000
      return version
    end
  end

  # unpack argument is N | Long, network (big-endian) byte order.
  # from http://ruby-doc.org/doxygen/1.8.4/pack_8c-source.html
  def receive
    raw_size = self.socket.recv(4)
    size = raw_size.unpack('N')
    
    # Read until we get to size
    read = 0
    buffer = ""
    
    while read < size[0] and size[0] > 0
      data = self.socket.recv(size[0] - read)
      buffer << data
      read += data.length
    end
    return buffer
  rescue
    self.reconnect!
  end

  # pack argument is N | Long, network (big-endian) byte order.
  # from http://ruby-doc.org/doxygen/1.8.4/pack_8c-source.html
  def send(request)
    self.reconnect unless self.socket
    bytes = request.serialize_to_string # helper method thanks to ruby-protobuf
    self.socket.write([bytes.size].pack("N") + bytes)
  rescue
    self.disconnect!
  end

  def send_protocol_version
    self.socket.write(self.protocol_version)
  end

  def protocol_handshake_ok?
    self.socket.recv(2) == STATUS_OK
  end

  def connect!
    self.connect_to_random_node
  end

  def reconnect!
    self.disconnect! if self.socket
    self.connect!
  end

  def disconnect!
    self.socket.close if self.socket
    self.socket = nil
  end
end
