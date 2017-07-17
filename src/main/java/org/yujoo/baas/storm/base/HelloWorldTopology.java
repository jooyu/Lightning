package org.yujoo.baas.storm.base;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class HelloWorldTopology {

	/**
	 * @param args
	 * @throws Exception  
	 * @throws  
	 */
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("randomHelloWorld", new HelloWorldSpout(), 2);        
        builder.setBolt("HelloWorldBolt", new HelloWorldBolt(), 10)
                .shuffleGrouping("randomHelloWorld");
                
        Config conf = new Config();
        conf.setDebug(true);
        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(20);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
        
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();    
        }

	}

}
