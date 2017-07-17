package org.yujoo.baas.storm.click;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class ClickTopology {

    private TopologyBuilder builder = new TopologyBuilder();
    private Config conf = new Config();
    private LocalCluster cluster;

    public static final String DEFAULT_JEDIS_PORT = "6379";

    public ClickTopology(){
        builder.setSpout("clickSpout", new ClickSpout(), 10);

        //First layer of bolts
        builder.setBolt("repeatsBolt", new RepeatVisitBolt(), 10)
                .shuffleGrouping("clickSpout");
        builder.setBolt("geographyBolt", new GeographyBolt(new HttpIPResolver()), 10)
                .shuffleGrouping("clickSpout");

        //second layer of bolts, commutative in nature
        builder.setBolt("totalStats", new VisitStatsBolt(), 1).globalGrouping("repeatsBolt");
        builder.setBolt("geoStats", new GeoStatsBolt(), 10).fieldsGrouping("geographyBolt", new Fields(org.yujoo.baas.storm.click.Fields.COUNTRY));
        conf.put(Conf.REDIS_PORT_KEY, DEFAULT_JEDIS_PORT);
    }

    public TopologyBuilder getBuilder() {
        return builder;
    }
    
    public LocalCluster getLocalCluster(){
    	return cluster;
    }

    public Config getConf() {
        return conf;
    }

    public void runLocal(int runTime){
        conf.setDebug(true);
        conf.put(Conf.REDIS_HOST_KEY, "192.168.141.119");
        cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        if(runTime > 0){
            Utils.sleep(runTime);
            shutDownLocal();
        }
    }

    public void shutDownLocal(){
        if(cluster != null){
    //暂不删除
//            cluster.killTopology("test");
//            cluster.shutdown();
        }
    }

    public void runCluster(String name, String redisHost) throws AlreadyAliveException, InvalidTopologyException {
        conf.setNumWorkers(20);
        conf.put(Conf.REDIS_HOST_KEY, redisHost);
       
        StormSubmitter.submitTopology(name, conf, builder.createTopology());
    }



	public static void main(String[] args) throws Exception {

        ClickTopology topology = new ClickTopology();
        

        if(args!=null && args.length > 1) {
            topology.runCluster(args[0], args[1]);
        } else {
            if(args!=null && args.length == 1)
                System.out.println("Running in local mode, redis ip missing for cluster run");
            topology.runLocal(10000);
        }

	}

}
