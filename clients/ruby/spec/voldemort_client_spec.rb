require File.dirname(__FILE__) + '/spec_helper'

include Voldemort

describe VoldemortClient do

  before(:each) do
    connection = mock(TCPConnection)
    connection.stub!(:key_serializer_type).and_return("string")
    connection.stub!(:value_serializer_type).and_return("string")
    node = mock(VoldemortNode)
    connection.stub!(:bootstrap).and_return(node)
    TCPConnection.stub!(:new).and_return(connection)
    @client = VoldemortClient.new("test", "localhost:6666")
    @client.stub!(:connection).and_return(connection)
  end

  describe "connection abstraction" do
   it "should have a connection" do
     @client.should respond_to(:connection)
   end

   it "should initialize the connection" do
     @client.connection.should_not be(nil)
   end
 end

 describe "default methods" do

   it "should support get" do
     @client.should respond_to(:get)
     version = mock(Versioned)
     v = mock(VectorClock)
     v.stub!(:value).and_return("some value")
     version.stub!(:versioned).and_return([v])
     @client.connection.should_receive(:get).with("key").and_return(version)
     @client.get("key").should eql("some value")
   end
   
   it "should support get all" do
     @client.should respond_to(:get_all)
     version = mock(Versioned)
     v = mock(VectorClock)
     v.stub!(:value).and_return("some value")
     v.stub!(:key).and_return("key")
     v.stub!(:versions).and_return([v])
     version.stub!(:values).and_return([v])
     @client.connection.should_receive(:get_all).with(["key", "key2"]).and_return(version)
     @client.get_all(["key", "key2"]).should eql({ "key" => "some value" }) # we pretend key2 doesn't exist
   end

   it "should support put" do
     @client.should respond_to(:put)
     @client.connection.should_receive(:put).with("key", "value").and_return("version")
     @client.put("key", "value").should eql("version")
   end

   it "should support delete" do
     @client.should respond_to(:delete)
     @client.connection.should_receive(:delete).with("key").and_return(true)
     @client.delete("key").should eql(true)
   end
 end

 describe "default resolver" do

   before(:each) do
     @old_versioned = Versioned.new
     @old_versioned.value = "old value"
     @old_versioned.version = VectorClock.new
     @old_versioned.version.timestamp = (Time.now-86400).to_i * 1000

     @new_versioned = Versioned.new
     @new_versioned.value = "new value"
     @new_versioned.version = VectorClock.new
     @new_versioned.version.timestamp = (Time.now).to_i * 1000

     @versions = []
     @versions << @old_versioned
     @versions << @new_versioned
   end

   it "should have a default resolver" do
     @client.should respond_to(:conflict_resolver)
   end

   it "should pick a default version form a list of versions, and should be the most recent value" do
     @client.resolve_conflicts(@versions).should eql(@new_versioned)
   end

   it "should allow a custom conflict resolver" do
     @client = VoldemortClient.new("test", "localhost:6666") do |versions|
       versions.first # just return the first version
     end
     @client.resolve_conflicts(@versions).should eql(@old_versioned)
   end
 end
end
