voldemort-rb
================

# Installing the Gem from rubygems

> sudo gem install voldemort-rb


# Requirements

Since the communication between the client and the server is done using protocol buffers you'll need the ruby_protobuf gem found at http://code.google.com/p/ruby-protobuf/. 

  sudo gem install ruby_protobuf

XML Parsing is done using Nokogiri

  sudo gem install nokogiri

# Building and Installing the Gem from source

> gem build voldemort-rb.gemspec

> sudo gem install voldemort-rb-0.1.X.gem (replace 'X' with the correct version)

Examples
=======

# Basic Usage
## Connecting and bootstrapping

  client = VoldemortClient.new("test", "localhost:6666")

## Storing a value

  client.put("some key", "some value")

## Reading a value

  client.get("some key")
  
  you'll get
  
  => some value

## deleting a value from a key

  client.delete("some key")

# Conflict resolution
## Default

Voldemort replies with versions of a value, it's up to the client to resolve the conflicts. By default the library will return the version that's most recent.

## Custom

You can override the default behavior and perform a custom resolution of the conflict, here's how to do so:

client = VoldemortClient.new("test", "localhost:6666") do |versions|

versions.first # just return the first version for example

end

Copyright (c) 2010 Alejandro Crosa, released under the MIT license
