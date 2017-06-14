package org.yujoo.baas.storm.base;

import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class HelloWorldSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = -4646687160233411001L;

	public static Logger LOG = Logger.getLogger(HelloWorldSpout.class);

	private SpoutOutputCollector collector;
	
	private int referenceRandom;
	
	private static final int MAX_RANDOM = 10;
	
	public HelloWorldSpout(){
		final Random rand = new Random();
		referenceRandom = rand.nextInt(MAX_RANDOM);
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void nextTuple() {
		Utils.sleep(100);
		final Random rand = new Random();
		int instanceRandom = rand.nextInt(MAX_RANDOM);
		if(instanceRandom == referenceRandom){
			collector.emit(new Values("Hello World"));
		} else {
			collector.emit(new Values("Other Random Word"));
		}
		

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
