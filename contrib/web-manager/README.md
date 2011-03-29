# Voldemort Web Manager #

The web manager use the Voldemort admin client to expose a web interface to Voldemort cluster management.  It supports:

* Creation of new stores using JSON serialization
* Listing stores
* Displaying JSON schema for a store
* Listing subset of entries from a store
* Changing bootstrap URL through UI

## Requirements ##

First install JRuby.  Then run the following command to install the required gems.

sudo jruby -S gem install sinatra emk-sinatra-url-for haml warbler sinatra-reloader

## Building ##

Simply build from the Voldemort root directory using "ant".  The web manager is written in Ruby but it needs the Voldemort JARs.  

## Running ##

Run the following command in the web-manager directory and point your browser to localhost:4567.

jruby init.rb

## Deploying ##

The web manager can be packaged as a WAR using the following command from the web-manager directory:

jruby -S warble