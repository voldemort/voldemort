require 'time'
require 'rexml/document'
include REXML

class VoldemortNode
  """A Voldemort node with the appropriate host and port information for contacting that node"""

  attr_reader :id, :host, :socket_port, :http_port, :partitions, :is_available, :last_contact

  def initialize(id, host, socket_port, http_port, partitions, is_available = true, last_contact = nil)
    @id = id
    @host = host
    @socket_port = socket_port
    @http_port = http_port
    @partitions = partitions
    @is_available = is_available
    if not last_contact
		  @last_contact = Time.new.to_f
    end
  end

  def inspect()
    return "node(id = #{@id}, host = #{@host}, socket_port = #{@socket_port}, http_port = #{@http_port}, partitions = #{@partitions.map {|i| i.to_s}.join(', ')})"
  end

  #@staticmethod
	def self.parse_cluster(xml)
		"""Parse the cluster.xml file and return a dictionary of the nodes in the cluster indexed by node id """
        doc = Document.new(xml)
		nodes = {}
        XPath.each(doc, '//server') do |curr|
          id = curr.elements['id'].text.to_i
          host = curr.elements['host'].text
          http_port = curr.elements['http-port'].text.to_i
          socket_port = curr.elements['socket-port'].text.to_i
          partition_str = curr.elements['partitions'].text
          partitions = partition_str.split(/, */).map {|p| p.to_i}
          nodes[id] = VoldemortNode.new(id, host, socket_port, http_port, partitions)
        end
        return nodes
    end
end