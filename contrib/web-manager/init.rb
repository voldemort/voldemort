# Require the necessary libraries.
require 'rubygems'
require 'sinatra'

gem 'emk-sinatra-url-for'
require 'sinatra/url_for'

# In production Warbler places all JAR files under lib. 
configure :production do
  puts "Loading production JARs"
  Dir["lib/*.jar"].each do |jar| 
    puts "requiring #{jar}"
    require jar 
  end
end

# In development mode we need to pick up JARs from various locations.
configure :development do |c|
  
  # reload when in development so we don't have to stop and start Sinatra
  require 'sinatra/reloader'
  
  puts "Loading development JARs"
  
  libs = []
  
  # Voldemort dependencies...
  libs << "../../lib/*.jar"
  
  # Voldemort JARs...
  libs << "../../dist/*.jar"

  libs.each do |lib|
    Dir[lib].each do |jar| 
      puts "requiring #{jar}"
      require jar 
      
      c.also_reload(jar)
    end
  end
end

# add controllers and views
configure do
  set :views, File.expand_path("app/views", File.dirname(__FILE__))
  enable :sessions
end

# Load the controllers.
Dir[File.expand_path("app/controllers/*.rb", File.dirname(__FILE__))].each { |file| load file }

before do
  session["bootstrap_host"] ||= request.host
  session["bootstrap_port"] ||= "6666"
  @bootstrap_host = session["bootstrap_host"]
  @bootstrap_port = session["bootstrap_port"]
  @bootstrap_url = @bootstrap_host + ":" + @bootstrap_port
end