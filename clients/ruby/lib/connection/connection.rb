require 'nokogiri'

class Connection

  attr_accessor :hosts          # The hosts from where we bootstrapped.
  attr_accessor :nodes          # The array of VoldemortNodes available.
  attr_accessor :db_name        # The DB store name.
  attr_accessor :connected_node # The VoldemortNode we are connected to.
  attr_accessor :request_count  # Used to track the number of request a node receives.
  attr_accessor :request_limit_per_node # Limit the number of request per node.
  attr_accessor :key_serializer_schemas
  attr_accessor :value_serializer_schemas
  attr_accessor :key_serializer_type
  attr_accessor :value_serializer_type

  STATUS_OK = "ok"
  PROTOCOL  = "pb0"
  DEFAULT_REQUEST_LIMIT_PER_NODE = 500

  def initialize(db_name, hosts, request_limit_per_node = DEFAULT_REQUEST_LIMIT_PER_NODE)
    self.db_name = db_name
    self.hosts   = hosts
    self.nodes   = hosts.collect{ |h|
                    n = h.split(":")
                    node = VoldemortNode.new
                    node.host = n[0]
                    node.port = n[1]
                    node
                   }
    self.request_count = 0
    self.request_limit_per_node = request_limit_per_node
  end

  def bootstrap
    cluster_response = self.get_from("metadata", "cluster.xml", false)
    cluster_xml_doc = Nokogiri::XML(cluster_response[1][0][1])
    self.nodes = self.parse_nodes_from(cluster_xml_doc)

    stores_response = self.get_from("metadata", "stores.xml", false)

    stores_xml = stores_response[1][0][1]

    doc = Nokogiri::XML(stores_xml)

    self.key_serializer_type = self.parse_schema_type(doc, 'key-serializer')
    self.value_serializer_type = self.parse_schema_type(doc, 'value-serializer')
    self.key_serializer_schemas = self.parse_schema_from(doc, 'key-serializer')
    self.value_serializer_schemas = self.parse_schema_from(doc, 'value-serializer')

    self.connect_to_random_node

  rescue StandardError => e
     raise("There was an error trying to bootstrap from the specified servers: #{e}")
  end

  def connect_to_random_node
    nodes = self.nodes.sort_by { rand }
    for node in nodes do
      if self.connect_to(node.host, node.port)
        self.connected_node = node
        self.request_count = 0
        return node
      end
    end
  end

  def parse_schema_type(doc, serializer = 'value-serializer')
    type_doc = doc.xpath("//stores/store[name = \"#{self.db_name}\"]/#{serializer}/type")
    if(type_doc != nil)
      return type_doc.text
    else
      return nil
    end
  end

  def parse_schema_from(doc, serializer = 'value-serializer')
    parsed_schemas = {}
    doc.xpath("//stores/store[name = \"#{self.db_name}\"]/#{serializer}/schema-info").each do |value_serializer|
      parsed_schemas[value_serializer.attributes['version'].text] = value_serializer.text
    end
    return parsed_schemas
  end

  def parse_nodes_from(doc)
    nodes = []
    doc.xpath("/cluster/server").each do |n|
      node = VoldemortNode.new      
      node.id = n.xpath("id").text
      node.host = n.xpath("host").text
      node.port = n.xpath("socket-port").text
      node.http_port = n.xpath("http_port").text
      node.admin_port = n.xpath("admin-port").text
      node.partitions = n.xpath("partitions").text
      nodes << node
    end
    nodes
  end

  def protocol_version
    PROTOCOL
  end

  def connect
    self.connect!
  end

  def reconnect
    self.reconnect!
  end

  def disconnect
    self.disconnect!
  end

  def reconnect_when_errors_in(response = nil)
    return unless response
    self.reconnect! if response.error
  end

  def rebalance_connection?
    self.request_count >= self.request_limit_per_node
  end

  def rebalance_connection_if_needed
    self.reconnect if self.rebalance_connection?
    self.request_count += 1
  end

  def get(key)
    self.rebalance_connection_if_needed
    self.get_from(self.db_name, key, false)
  end

  def get_all(keys)
    self.rebalance_connection_if_needed
    self.get_all_from(self.db_name, keys, false)
  end

  def put(key, value, version = nil, route = false)
    self.rebalance_connection_if_needed
    self.put_from(self.db_name, key, value, version, route)
  end
  
  def delete(key)
    self.delete_from(self.db_name, key)
  end
end
