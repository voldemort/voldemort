require File.dirname(__FILE__) + '/spec_helper'

describe VoldemortNode do

  before(:each) do
    @voldemort_node = VoldemortNode.new
  end

  describe "default methods" do

    it "should have id, host, port, http_port, admin_port and partitions" do
      [:id, :host, :port, :http_port, :admin_port, :partitions].each do |m|
        @voldemort_node.should respond_to(m)
      end
    end
  end
end