require 'rubygems'

include Java

include_class Java::voldemort.serialization.SerializerDefinition
include_class Java::voldemort.store.StoreDefinition
include_class Java::voldemort.client.RoutingTier
include_class Java::voldemort.client.protocol.admin.AdminClient
include_class Java::voldemort.client.protocol.admin.AdminClientConfig
include_class Java::voldemort.client.protocol.admin.filter.DefaultVoldemortFilter
include_class Java::voldemort.serialization.DefaultSerializerFactory

module VoldemortAdmin

  class StoreInfo
    attr_accessor :name, :key_info, :value_info
    
    def self.from_def(store_def)  
      info = StoreInfo.new
      info.name = store_def.name
      info.key_info = SerializerInfo.from_def(store_def.key_serializer)
      info.value_info = SerializerInfo.from_def(store_def.value_serializer)
      info
    end
    
    def <=>(other)
      self.name <=> other.name
    end
  end
  
  class SerializerInfo
    attr_accessor :name, :schema
    
    def self.from_def(serializer_def)
      info = SerializerInfo.new
      info.name = serializer_def.name
      info.schema = serializer_def.current_schema_info if serializer_def.has_schema_info
      info
    end
  end
  
  class AdminProxy
    attr_reader :host, :port
  
    def initialize(host, port)
      @host = host
      @port = port
    end
    
    def url
      @host + ":" + @port
    end
    
    def create_store(store_info)    
      key_def = SerializerDefinition.new(store_info.key_info.name, store_info.key_info.schema)
      value_def = SerializerDefinition.new(store_info.value_info.name, store_info.value_info.schema)
    
      store_def = StoreDefinition.new(
        store_info.name,
        "bdb", #type
        key_def,
        value_def,
        RoutingTier::CLIENT,
        "consistent-routing", #routingStrategyType
        1, #replicationFactor
        1, #preferredReads 
        1, #requiredReads 
        1, #preferredWrites
        1, #requiredWrites
        nil, #viewOfStore
        nil, #valTrans
        nil, #zoneReplicationFactor
        nil, #zoneCountReads
        nil, #zoneCountWrites
        nil, #retentionDays 
        nil #retentionThrottleRate
        )
        
        client.add_store(store_def)
    end
    
    def store(name)
      store = stores.find { |store| store.name == name }
      store
    end
    
    def entries(name, max=nil)
      defs = client.remote_store_def_list(0).value
      partitions = cluster.node_by_id(0).partition_ids
      store_def = defs.find { |d| d.name == name }
      result = []
      if store_def
        key_serializer = store_def.key_serializer
        value_serializer = store_def.value_serializer
        
        entries = client.fetch_entries(0, name, partitions, DefaultVoldemortFilter.new, false)
        
        begin
          has_any = entries.has_next?
        rescue
        end
        
        if !has_any.nil? && has_any
          while entries.has_next?
            break if !max.nil? and result.size >= max
            
            entry = entries.next
            key_bytes = entry.first.get
            value_bytes = entry.second.value
          
            factory = DefaultSerializerFactory.new
          
            key = factory.get_serializer(key_serializer).to_object(key_bytes)
            value = factory.get_serializer(value_serializer).to_object(value_bytes)
          
            result << [key, value]
          end   
        end     
      end
      result
    end
    
    def stores
      defs = client.remote_store_def_list(0).value
      defs.map do |store_def|
        StoreInfo.from_def(store_def)
      end
    end
    
    private
    
    def cluster
      @cluster ||= client.admin_client_cluster
    end
    
    def client
      @client ||= AdminClient.new("tcp://" + url, AdminClientConfig.new)
    end
  end
end