package edu.upenn.cis.stormlite.bolt;

import java.util.Map;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;

public class FilterBolt implements IRichBolt {

	@Override
	public String getExecutorId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setRouter(IStreamRouter router) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Fields getSchema() {
		// TODO Auto-generated method stub
		return null;
	}

}
