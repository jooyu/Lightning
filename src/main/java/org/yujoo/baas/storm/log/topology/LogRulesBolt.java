package org.yujoo.baas.storm.log.topology;

import java.util.Map;

import org.apache.log4j.Logger;
import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseFactory;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.ResourceType;
import org.drools.io.ResourceFactory;
import org.drools.runtime.StatelessKnowledgeSession;
import org.yujoo.baas.storm.log.topology.model.LogEntry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LogRulesBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	public static Logger LOG = Logger.getLogger(LogRulesBolt.class);
	private StatelessKnowledgeSession ksession;
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		//TODO: load the rule definitions from an external agent instead of the classpath.
		KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
		kbuilder.add( ResourceFactory.newClassPathResource( "/Syslog.drl", 
		              getClass() ), ResourceType.DRL );
		if ( kbuilder.hasErrors() ) {
		    LOG.error( kbuilder.getErrors().toString() );
		}
		KnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase();
		kbase.addKnowledgePackages( kbuilder.getKnowledgePackages() );	
		ksession = kbase.newStatelessKnowledgeSession();
	}

	@Override
	public void execute(Tuple input) {
		LogEntry entry = (LogEntry)input.getValueByField(FieldNames.LOG_ENTRY);
		if(entry == null){
			LOG.fatal( "Received null or incorrect value from tuple" );
			return;
		}
		ksession.execute( entry );
		if(!entry.isFilter()){
			LOG.debug("Emitting from Rules Bolt");
			collector.emit(new Values(entry));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.LOG_ENTRY));
	}

}
