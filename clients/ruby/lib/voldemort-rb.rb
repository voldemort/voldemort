libdir = File.dirname(__FILE__)
$LOAD_PATH.unshift(libdir) unless $LOAD_PATH.include?(libdir)

require 'connection/voldemort_node' 
require 'connection/connection' 
require 'connection/tcp_connection' 
require 'voldemort-serializer'

class VoldemortClient
  attr_accessor :connection
  attr_accessor :conflict_resolver
  attr_accessor :key_serializer
  attr_accessor :value_serializer

  def initialize(db_name, *hosts, &block)
    self.conflict_resolver = block unless !block
    self.connection = TCPConnection.new(db_name, hosts) # implement and modifiy if you don't want to use TCP protobuf.
    self.connection.bootstrap
    
    case(self.connection.key_serializer_type)
      when 'json'
        self.key_serializer = VoldemortJsonBinarySerializer.new(self.connection.key_serializer_schemas)
      else
        self.key_serializer = VoldemortPassThroughSerializer.new({})
    end
    
    case(self.connection.value_serializer_type)
      when 'json'
        self.value_serializer = VoldemortJsonBinarySerializer.new(self.connection.value_serializer_schemas)
      else
        self.value_serializer = VoldemortPassThroughSerializer.new({})
    end
  end

  def get(key)
    versions = self.connection.get(key_serializer.to_bytes(key))
    version  = self.resolve_conflicts(versions.versioned)
    if version
      value_serializer.to_object(version.value)
    else
      nil
    end
  end

  def get_all(keys)
    serialized_keys = []
    
    keys.each do |key|
      serialized_keys << key_serializer.to_bytes(key)
    end
    
    all_version = self.connection.get_all(keys)
    values = {}
    all_version.values.collect do |v|
      values[v.key] = value_serializer.to_object(self.resolve_conflicts(v.versions).value)
    end
    values
  end

  def put(key, value, version = nil)
    self.connection.put(key_serializer.to_bytes(key), value_serializer.to_bytes(value), version)
  end

  def delete(key)
    self.connection.delete(key_serializer.to_bytes(key))
  end

  def resolve_conflicts(versions)
    return self.conflict_resolver.call(versions) if self.conflict_resolver
    # by default just return the version that has the most recent timestamp.
    versions.max { |a, b| a.version.timestamp <=> b.version.timestamp }
  end
end
