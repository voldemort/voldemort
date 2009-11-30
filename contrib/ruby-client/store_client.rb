require 'rubygems'
require 'logger'
require 'socket'
require 'node'
require 'voldemort_exception'
require 'voldemort-client.pb.rb'
include Voldemort

$LOG = Logger.new(STDOUT)

# A simple Voldemort client. It is single-threaded and supports only server-side routing.
class StoreClient
  def initialize(store_name, bootstrap_urls, reconnect_interval = 500, conflict_resolver = nil)
    @store_name = store_name
    @request_count = 0
    @conflict_resolver = conflict_resolver
    @nodes = _bootstrap_metadata(bootstrap_urls)
    @node_id = rand(@nodes.size)
    @node_id, @connection = _reconnect()
    @reconnect_interval = reconnect_interval
    @open = true
  end

  def _make_connection(host, port)
    protocol = 'pb0'
    $LOG.debug("Attempting to connect to #{host}:#{port}")
    connection = TCPSocket.open(host, port)
    $LOG.debug('Connection succeeded, negotiating protocol')
    connection.send(protocol, 0)
    resp = connection.recv(2)
    if resp != 'ok'
      raise VoldemortException.new("Server does not understand the protocol #{protocol}")
      end
    return connection
  end

  ## Connect to a the next available node in the cluster
  ## returns a tuple of (node_id, connection)
  def _reconnect()
    num_nodes = @nodes.size
    attempts = 0
    new_node_id = @node_id
    while attempts < num_nodes
      new_node_id = (new_node_id + 1) % num_nodes
      new_node = @nodes[new_node_id]
      connection = nil
      begin
        connection = _make_connection(new_node.host, new_node.socket_port)
        @request_count = 0
        return new_node_id, connection
      rescue SocketError => message
        _close_socket(connection)
        connection.close
        $LOG.warn("Error connecting to node #{new_node_id}: #{message}")
        attempts += 1
      end
    end

    # If we get here all nodes have failed us, explode
    raise VoldemortException.new('Connections to all nodes failed.')
  end

  ## Safely close the socket, catching and logging any exceptions
  def _close_socket(socket)
		begin
			if socket
              socket.close
            end
        rescue SocketError => message
           $LOG.error("Error while closing socket: #{message}")
        end
  end

  ## Check if the the number of requests made on this connection is greater than the reconnect interval.
	## If so reconnect to a random node in the cluster. No attempt is made at preventing the reconnecting
	## from going back to the same node
	def _maybe_reconnect()
		if @request_count >= @reconnect_interval
			$LOG.debug("Completed #{@request_count} requests using this connection, reconnecting...")
			_close_socket(@connection)
			@node_id, @connection = _reconnect()
        end
    end

  ## send a request to the server using the given connection
	def _send_request(connection, req_bytes)
		connection.print([req_bytes.size].pack('N') + req_bytes)
		@request_count += 1
    end

  ## read a response from the connection
	def _receive_response(connection)
		size_bytes = connection.recv(4)
		size = size_bytes.unpack('N')

		if size[0] == 0
			return ''
        end

		return connection.recv(size[0]) 
    end

  ## Bootstrap cluster metadata from a list of urls of nodes in the cluster.
  ## The urls are tuples in the form (host, port).
  ## A dictionary of node_id => node is returned.
  def _bootstrap_metadata(bootstrap_urls)
    bootstrap_urls.sort_by { rand }
    for host, port in bootstrap_urls
      $LOG.debug("Attempting to bootstrap metadata from #{host}:#{port}")
      connection = nil
      begin
        connection = _make_connection(host, port)
        cluster_xmls = _get_with_connection(connection, 'metadata', 'cluster.xml', false)
        if cluster_xmls.size != 1
          raise VoldemortException.new("Expected exactly one version of the metadata but found #{cluster_xmls}")
        end
        nodes = VoldemortNode.parse_cluster(cluster_xmls[0][0])
        $LOG.debug("Bootstrap from #{host}:#{port} succeeded, found #{nodes.size} nodes.")
        return nodes
      rescue SocketError => message
        $LOG.warn("Metadata bootstrap from #{host}:#{port} failed: #{message}")
      ensure
        _close_socket(connection)
      end
    end
    raise VoldemortException.new('All bootstrap attempts failed')
  end

  ## check if the server response has an error, if so throw an exception
	def _check_error(resp)
		if resp.error
			raise VoldemortException.new(resp.error.error_message, resp.error.error_code)
        end
    end

  ## Increment the version for a vector clock
	def _increment(clock)
		# See if we already have a version for this guy, if so increment it
		for entry in clock.entries
			if entry.node_id == @node_id
			 	entry.version += 1
			 	return
            end
        end
		# Otherwise add a version
        entry = ClockEntry.new
		entry.node_id = @node_id
		entry.version = 1
        clock.entries << entry
		clock.timestamp = Time.new.to_i * 1000
    end

  ## Take a list of versions, and, if a conflict resolver has been given, resolve any conflicts that can be resolved
	def _resolve_conflicts(versions)
		if @conflict_resolver and versions
			return conflict_resolver(versions)
		else
			return versions
        end
    end

  ## Turn a protocol buffer list of versioned items into a ruby list of items
	def _extract_versions(pb_versioneds)
		versions = []
		for versioned in pb_versioneds
			versions << [versioned.value, versioned.version]
        end
		return _resolve_conflicts(versions)
    end

  ## A basic request wrapper, that handles reconnection logic and failures
	def _execute_request(fun, args)
		raise 'Store has been closed.' unless @open
		_maybe_reconnect()

		failures = 0
		num_nodes = @nodes.size
		while failures < num_nodes
			begin
				return send(fun, *args)
			rescue SocketError => message
				$LOG.warn("Error while performing #{fun} on node #{@node_id}: #{message}")
				_reconnect()
				failures += 1
            end
        end

		raise VoldemortException.new("All nodes are down, #{fun} failed.")
    end

  ## An internal get function that take the connection and store name as parameters. This is
	## used by both the public get() method and also the metadata bootstrap process
	def _get_with_connection(connection, store_name, key, should_route)
		"""Execute get request to the given store. Returns a (value, version) pair."""

		req = VoldemortRequest.new
		req.should_route = should_route
		req.store = store_name
		req.type = RequestType::GET
        req.get = GetRequest.new
		req.get.key = key

		# send request
		_send_request(connection, req.serialize_to_string())

		# read and parse response
		resp_str = _receive_response(connection)
		resp = GetResponse.new
		resp = resp.parse_from_string(resp_str)
		_check_error(resp)

		return _extract_versions(resp.versioned)
    end

  ## Inner helper function for get
	def _get(key)
		return _get_with_connection(@connection, @store_name, key, true)
    end

	def get(key)
		#"""Execute a get request. Returns a list of (value, version) pairs."""
		return _execute_request(:_get, [key])
    end

  ## Inner get_all method that takes the connection and store_name as parameters
	def _get_all(keys)
		req = VoldemortRequest.new
		req.should_route = true
		req.store = @store_name
		req.type = RequestType::GET_ALL
        req.getAll = GetAllRequest.new
		for key in keys
			req.getAll.keys << key
        end

		# send request
		_send_request(@connection, req.serialize_to_string())

		# read and parse response
		resp_str = _receive_response(@connection)
		resp = GetAllResponse.new
		resp = resp.parse_from_string(resp_str)
		_check_error(resp)
		values = {}
		for key_val in resp.values
			values[key_val.key] = _extract_versions(key_val.versions)
        end
		return values
    end

  def get_all(keys)
		#"""Execute get request for multiple keys given as a list or tuple.
		#   Returns a dictionary of key => [(value, version), ...] pairs."""
		return _execute_request(:_get_all, [keys])
  end

  ## Get the current version of the given key by doing a get request to the store
	def _fetch_version(key)
		versioned = get(key)
		if versioned.length > 0
             version = versioned[0][1]
        else
          begin
            version = VectorClock.new
			version.timestamp = Time.new.to_i * 1000
          end
        end
		return version
    end

  def _put(key, value, version)
		req = VoldemortRequest.new
		req.should_route = true
		req.store = @store_name
		req.type = RequestType::PUT
        req.put = PutRequest.new
		req.put.key = key
        req.put.versioned = Versioned.new
		req.put.versioned.value = value
        req.put.versioned.version = VectorClock.new
		req.put.versioned.version.merge_from(version)

		# send request
		_send_request(@connection, req.serialize_to_string())

		# read and parse response
		resp_str = _receive_response(@connection)
		resp = PutResponse.new
		resp = resp.parse_from_string(resp_str)
		_check_error(resp)
		_increment(version)
		return version
  end

  def put(key, value, version = nil)
		#"""Execute a put request using the given key and value. If no version is specified a get(key) request
		#   will be done to get the current version. The updated version is returned."""
		# if we don't have a version, fetch one
		if not version
			version = _fetch_version(key)
        end
        return _execute_request(:_put, [key, value, version])
  end

  def maybe_put(key, value, version = nil)
		#"""Execute a put request using the given key and value. If the version being put is obsolete,
		#   no modification will be made and this function will return None. Otherwise it will return the new version."""
		begin
			return put(key, value, version)
		rescue
			return nil
        end
  end

  def _delete(key, version)
		req = VoldemortRequest.new
		req.should_route = true
		req.store = @store_name
		req.type = RequestType::DELETE
        req.delete = DeleteRequest.new
		req.delete.key = key
        req.delete.version = VectorClock.new
		req.delete.version.merge_from(version)

		# send request
		_send_request(@connection, req.serialize_to_string())

		# read and parse response
		resp_str = _receive_response(@connection)
		resp = DeleteResponse.new
        resp = resp.parse_from_string(resp_str)
		_check_error(resp)

		return resp.success
  end

  def delete(key, version = nil)
		#"""Execute a delete request, deleting all keys up to and including the given version.
		#   If no version is given a get(key) request will be done to find the latest version."""
		# if we don't have a version, fetch one
		if version == nil
			version = _fetch_version(key)
        end
		return _execute_request(:_delete, [key, version])
  end

  def close()
      #"""Close the connection this store maintains."""
    @open = false
    @connection.close()
  end

end
