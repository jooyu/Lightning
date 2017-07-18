//package org.yujoo.baas.storm.online.ml.topology;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//
//
//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;
//import org.json.simple.JSONValue;
//
//import storm.kafka.KafkaConfig.StaticHosts;
//import storm.kafka.trident.TransactionalTridentKafkaSpout;
//import storm.kafka.trident.TridentKafkaConfig;
//import storm.trident.TridentState;
//import storm.trident.TridentTopology;
//import storm.trident.operation.BaseFunction;
//import storm.trident.operation.TridentCollector;
//import storm.trident.testing.MemoryMapState;
//import storm.trident.tuple.TridentTuple;
//import backtype.storm.Config;
//import backtype.storm.LocalCluster;
//import backtype.storm.StormSubmitter;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Values;
//
//import com.github.pmerienne.trident.ml.preprocessing.InstanceCreator;
//import com.github.pmerienne.trident.ml.regression.PerceptronRegressor;
//import com.github.pmerienne.trident.ml.regression.RegressionQuery;
//import com.github.pmerienne.trident.ml.regression.RegressionUpdater;
//import com.github.quintona.KafkaState;
//import com.github.quintona.KafkaStateUpdater;
//
//public class OnlineTopology {
//	
//	public static class CoerceInSample extends BaseFunction {
//		@Override
//		public void execute(TridentTuple tuple, TridentCollector collector) {
//	    	String text = new String(tuple.getBinary(0));
//	    	JSONArray array = (JSONArray) JSONValue.parse(text);
//	    	Double[] values = new Double[array.size()];
//	    	for(int i = 0; i < array.size(); i++){
//	    		values[i] = ((Number)array.get(i)).doubleValue();
//	    	}
//	    	if(array.size() > 0){
//				collector.emit(new Values(values));
//	    	}
//		}
//	}
//	
//	public static class CoerceInTransaction extends BaseFunction {
//		@Override
//		public void execute(TridentTuple tuple, TridentCollector collector) {
//	    	String text = new String(tuple.getBinary(0));
//	    	JSONArray array = (JSONArray) JSONValue.parse(text);
//	    	String id = (String)array.remove(array.size() - 1);
//	    	List<Object> values = new ArrayList<Object>(array.size());
//	    	for(int i = 0; i < array.size(); i++){
//	    		values.add(((Number)array.get(i)).doubleValue());
//	    	}
//	    	values.add(id);
//	    	if(array.size() > 0){
//				collector.emit(new Values(values.toArray()));
//	    	}
//		}
//	}
//	
//	public static class CoerceOutFunction extends BaseFunction {
//		@Override
//		public void execute(TridentTuple tuple, TridentCollector collector) {
//	    	JSONObject obj = new JSONObject();
//	    	obj.put("transaction-id", tuple.getStringByField("transaction-id"));
//	    	obj.put("prediction", tuple.getDoubleByField("prediction"));
//	    	collector.emit(new Values(obj.toJSONString()));
//		}
//	}
//
//	public static TridentTopology makeTopology() throws IOException {
//		TridentTopology topology = new TridentTopology();
//		
//		TridentKafkaConfig trainingSpoutConfig = new TridentKafkaConfig(
//				StaticHosts.fromHostString(
//						Arrays.asList(new String[] { "localhost" }), 2), "labeled_data");
//		
//		TridentKafkaConfig scoringSpoutConfig = new TridentKafkaConfig(
//				StaticHosts.fromHostString(
//						Arrays.asList(new String[] { "localhost" }), 2), "transactions");
//
//		TridentState perceptronModel = topology 
//				  .newStream("labeleddata", new TransactionalTridentKafkaSpout(trainingSpoutConfig))
//				  .each(new Fields("bytes"), new CoerceInSample(),new Fields("f1","f2","f3","f4","label"))
//				  .each(new Fields("label", "f1","f2","f3","f4"), new InstanceCreator<Double>(), new Fields("instance"))
//				  .partitionPersist(new MemoryMapState.Factory(), new Fields("instance"), 
//						  new RegressionUpdater("perceptron", new PerceptronRegressor()));
//		
//		topology.newStream("transactions",
//				new TransactionalTridentKafkaSpout(scoringSpoutConfig))
//					.each(new Fields("bytes"), new CoerceInTransaction(),new Fields("f1","f2","f3","f4","transaction-id"))
//					.each(new Fields("f1","f2","f3","f4"), new InstanceCreator<Double>(false), new Fields("instance"))
//					.stateQuery(perceptronModel, new Fields("instance"), new RegressionQuery("perceptron"), new Fields("prediction"))
//					.each(new Fields("transaction-id", "prediction"), 
//							new CoerceOutFunction(),new Fields("message"))
//					.partitionPersist(KafkaState.transactional("prediction-output", 
//							new KafkaState.Options()), new Fields("message"), 
//							new KafkaStateUpdater("message"), new Fields());
//
//		return topology;
//	}
//
//	public static void main(String[] args) throws Exception {
//		Config conf = new Config();
//		conf.setDebug(true);
//		
//		if (args != null && args.length > 0) {
//			conf.setNumWorkers(3);
//			//TODO: get count from the args.
//			StormSubmitter
//					.submitTopology(args[0], conf, makeTopology().build());
//		} else {
//			conf.setMaxTaskParallelism(3);
//			conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(new String[]{"127.0.0.1"}));
//			conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
//			conf.put(Config.STORM_ZOOKEEPER_ROOT, "/storm");
//			LocalCluster cluster = new LocalCluster();
//			cluster.submitTopology("online-topology", conf,
//					makeTopology().build());
//		}
//	}
//}
