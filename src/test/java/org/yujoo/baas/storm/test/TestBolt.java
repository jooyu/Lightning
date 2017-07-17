package org.yujoo.baas.storm.test;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.yujoo.baas.storm.click.ClickTopology;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public final class TestBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private static final transient Logger LOG = Logger.getLogger(TestBolt.class);
	
	private static Jedis jedis;
	
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    	jedis = new Jedis("192.168.141.119", Integer.parseInt(ClickTopology.DEFAULT_JEDIS_PORT));
    	jedis.auth("123456");
    	jedis.connect();
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	@Override
	public void execute(Tuple input) {
		List objects = input.getValues();
		objects.add(0, input.getSourceComponent());
		jedis.rpush("TestTuple", JSONArray.toJSONString(objects));
		
	}
    
}
