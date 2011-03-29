require 'rubygems'
require 'sinatra'
require 'haml'

include Java

get '/' do
  redirect url_for '/stores'
end

get '/stores' do
  begin
    proxy = VoldemortAdmin::AdminProxy.new(@bootstrap_host, @bootstrap_port)
    @stores = proxy.stores
  rescue
  end
  unless @stores.nil?    
    @stores.sort!
    haml :index
  else
    haml :bad_url
  end
end

get '/store/:name' do |name|
  @name = name
  proxy = VoldemortAdmin::AdminProxy.new(@bootstrap_host, @bootstrap_port)
  @store = proxy.store(name)
  halt 404 unless @store
  @limit = 25
  fetch_count = @limit + 1
  @entries = proxy.entries(name, fetch_count)
  @has_more = @entries.size >= fetch_count
  @entries = @entries.take(@limit)
  haml :store
end

include_class Java::voldemort.client.SocketStoreClientFactory
include_class Java::voldemort.client.ClientConfig

get '/store/:name/:key' do |name, key|
  config = ClientConfig.new
  config.setBootstrapUrls("tcp://" + @bootstrap_url)
  factory = SocketStoreClientFactory.new(config)
  client = factory.getStoreClient(name)
  
  proxy = VoldemortAdmin::AdminProxy.new(@bootstrap_host, @bootstrap_port)
  @store = proxy.store(name)
  key_schema = @store.key_info.schema
  
  # TODO: This only supports keys which are int32 or strings.  Figure out how a string
  # can be passed from the browser and converted to the appropriate type before calling
  # client.getValue.
  if (key_schema =~ /int32/)
    client.getValue(java.lang.Integer.new(key.to_i)).to_s  
  else
    client.getValue(key).to_s
  end
end

get '/stores/new' do
  haml :store_new
end

require 'app/helpers/VoldemortAdmin'

post '/stores/new' do
  key_info = VoldemortAdmin::SerializerInfo.new
  key_info.name = params[:store_key_name]
  key_info.schema = params[:store_key_schema]
  
  value_info = VoldemortAdmin::SerializerInfo.new
  value_info.name = params[:store_value_name]
  value_info.schema = params[:store_value_schema]
  
  store_info = VoldemortAdmin::StoreInfo.new
  store_info.name = params[:store_name]
  store_info.key_info = key_info
  store_info.value_info = value_info
  
  proxy = VoldemortAdmin::AdminProxy.new(@bootstrap_host, @bootstrap_port)
  proxy.create_store(store_info)
  
  redirect url_for '/stores'
end

get '/config' do
  haml :config
end

post '/config' do
  session["bootstrap_host"] = params["bootstrap_host"]
  session["bootstrap_port"] = params["bootstrap_port"]
  redirect url_for '/stores'
end