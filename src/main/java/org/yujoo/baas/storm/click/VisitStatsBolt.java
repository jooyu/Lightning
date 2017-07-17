package org.yujoo.baas.storm.click;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class VisitStatsBolt extends BaseRichBolt {

    private OutputCollector collector;

    private int total = 0;
    private int uniqueCount = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        boolean unique = Boolean.parseBoolean(tuple.getStringByField(org.yujoo.baas.storm.click.Fields.UNIQUE));
        total++;
        if(unique)uniqueCount++;
        collector.emit(new Values(total,uniqueCount));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new backtype.storm.tuple.Fields(Fields.TOTAL_COUNT,
                Fields.TOTAL_UNIQUE));
    }
}
