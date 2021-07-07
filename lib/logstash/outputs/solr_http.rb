# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "stud/buffer"
require "rubygems"
require "uuidtools"


# This output lets you index&store your logs in Solr. If you want to get
# started quickly you should use version 4.4 or above in schemaless mode,
# which will try and guess your fields automatically. To turn that on,
# you can use the example included in the Solr archive:
# [source,shell]
#     tar zxf solr-4.4.0.tgz
#     cd example
#     mv solr solr_ #back up the existing sample conf
#     cp -r example-schemaless/solr/ .  #put the schemaless conf in place
#     java -jar start.jar   #start Solr
#
# You can learn more at https://lucene.apache.org/solr/[the Solr home page]

class LogStash::Outputs::SolrHTTP < LogStash::Outputs::Base
  include Stud::Buffer

  config_name "solr_http"


  # URL used to connect to Solr
  config :solr_url, :validate => :string, :default => "http://localhost:8983/solr"

  # Number of events to queue up before writing to Solr
  config :flush_size, :validate => :number, :default => 100

  # Amount of time since the last flush before a flush is done even if
  # the number of buffered events is smaller than flush_size
  config :idle_flush_time, :validate => :number, :default => 1

  # Solr document ID for events. You'd typically have a variable here, like
  # '%{foo}' so you can assign your own IDs
  config :document_id, :validate => :string, :default => nil

  # Solr field name of document ID field name.
  config :document_id_field, :validate => :string, :default => "id"
  
  public
  def register
    require "rsolr"
    buffer_initialize(
      :max_items => @flush_size,
      :max_interval => @idle_flush_time,
      :logger => @logger
    )
  end #def register

  public
  def receive(event)
    buffer_receive(event)
  end #def receive

  public
  def flush(events, close=false)
    documents = Hash.new   #this is the map of hashes that we push to Solr as documents
    events.each do |event|
        url = event.sprintf(@solr_url)  # solr url sprintf

        if documents[url].nil? 
          documents[url]=[]  # create a new array to url
          @logger.debug("new url created [#{url}]")
        end        
        document = event.to_hash()        
        if @document_id.nil?
          document [@document_id_field] = UUIDTools::UUID.random_create    #add a unique ID
        else
          document [@document_id_field] = event.sprintf(@document_id)      #or use the one provided
        end
        documents[url].push(document)
    end
    @logger.debug("#{documents.keys.length()} url detected")
    documents.keys.each do |url|
      solr = RSolr.connect :url => url
      @logger.debug("solr connected [#{url}]")
      @logger.debug("#{documents[url].length()} documents indexing...")
      solr.add(documents[url])
      @logger.debug("#{documents[url].length()} documents indexed.")
      solr.commit :commit_attributes => {}
    end

    rescue Exception => e
      @logger.warn("An error occurred while indexing: #{e.message}")
  end #def flush
end #class LogStash::Outputs::SolrHTTP
