Gem::Specification.new do |s|
  s.name = 'voldemort-rb'
  s.version = '0.1.5'
  s.summary = %{A Ruby client for the Voldemort distributed key value store}
  s.description = %Q{voldemort-rb allows you to connect to the Voldemort descentralized key value store.}
  s.authors = ["Alejandro Crosa"]
  s.email = ["alejandrocrosa@gmail.com"]
  s.homepage = "http://github.com/acrosa/voldemort-rb"
  s.files = [
       "CHANGELOG",
       "LICENSE",
       "README.md",
       "Rakefile",
       "lib/voldemort-rb.rb",
       "lib/voldemort-serializer.rb",
       "lib/connection/connection.rb",
       "lib/connection/tcp_connection.rb",
       "lib/connection/voldemort_node.rb",
       "lib/protos/voldemort-client.pb.rb",
       "lib/protos/voldemort-client.proto",
       "spec/connection_spec.rb",
       "spec/tcp_connection_spec.rb",
       "spec/voldemort_node_spec.rb",
       "spec/voldemort_client_spec.rb",
       "spec/spec_helper.rb"
  ]
  s.require_paths = ["lib"]
  s.add_dependency('ruby_protobuf', '>= 0.3.3')
  s.add_dependency('nokogiri', '>= 1.4.3.1')
end
