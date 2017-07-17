package org.yujoo.baas.storm.log.topology;

import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.yujoo.baas.storm.log.topology.model.LogEntry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class IndexerBolt extends BaseRichBolt {

	private Client client;
	public static Logger LOG = Logger.getLogger(LogRulesBolt.class);
	private OutputCollector collector;
	
	public static final String INDEX_NAME = "logstorm";
	public static final String INDEX_TYPE = "logentry";
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		Node node;
		if((Boolean)stormConf.get(backtype.storm.Config.TOPOLOGY_DEBUG) == true){
			node = NodeBuilder.nodeBuilder().local(true).node();
		} else {
			String clusterName = (String) stormConf.get(Conf.ELASTIC_CLUSTER_NAME);
			if(clusterName == null)
				clusterName = Conf.DEFAULT_ELASTIC_CLUSTER;
			node = NodeBuilder.nodeBuilder().clusterName(clusterName).node();
		}
		client = node.client();
	}

	@Override
	public void execute(Tuple input) {
		LogEntry entry = (LogEntry)input.getValueByField(FieldNames.LOG_ENTRY);
		if(entry == null){
			LOG.fatal( "Received null or incorrect value from tuple" );
			return;
		}
		String toBeIndexed = entry.toJSON().toJSONString();
		IndexResponse response = client.prepareIndex(INDEX_NAME,INDEX_TYPE)
				.setSource(toBeIndexed)
				.execute().actionGet();
		if(response == null)
			LOG.error("Failed to index Tuple: " + input.toString());
		else{
			if(response.getId() == null)
				LOG.error("Failed to index Tuple: " + input.toString());
			else{ 
				LOG.debug("Indexing success on Tuple: " + input.toString());
				collector.emit(new Values(entry,response.getId()));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.LOG_ENTRY, FieldNames.LOG_INDEX_ID));
	}

}
