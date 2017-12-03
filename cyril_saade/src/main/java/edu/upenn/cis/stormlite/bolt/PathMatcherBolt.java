package edu.upenn.cis.stormlite.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;

import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.cis455.model.OccurrenceEvent;
import edu.upenn.cis.cis455.xpathengine.XPathEngineFactory;
import edu.upenn.cis.cis455.xpathengine.XPathEngine;
import edu.upenn.cis.cis455.storage.StorageInterface;

import edu.upenn.cis.stormlite.CrawlerFactory;
import edu.upenn.cis.cis455.crawler.CrawlMaster;

/**
 * Bolt that will read an occurance event
 * Using XPathEngine it will check if there is a match
 * If there is a match, it will update BerkleyDB
 */
public class PathMatcherBolt implements IRichBolt {
    
    static Logger log = LogManager.getLogger(PathMatcherBolt.class);
	
	Fields schema = new Fields("");
    String executorId = UUID.randomUUID().toString();
    private OutputCollector collector;
    private XPathEngine xpathEngine;
    private StorageInterface db;
    private String[] channelNames;
    private CrawlMaster master;
    
    public PathMatcherBolt() {
        db = CrawlerFactory.getDatabaseInstance();
        xpathEngine = XPathEngineFactory.getXPathEngine();
        master = CrawlerFactory.getCrawlMasterInstance();
    }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        
        int i = 0;
        String[] expressions = new String[db.getAllXPaths().keySet().size()];
        channelNames = new String[db.getAllXPaths().keySet().size()];
        for(String channelName: db.getAllXPaths().keySet()) {
            expressions[i] = db.getXpath(channelName);
            channelNames[i] = channelName;
            i++;
        }
        xpathEngine.setXPaths(expressions);
    }

    /**
     * Process a tuple received from the stream, incrementing our
     * counter and outputting a result
     */
    @Override
    public void execute(Tuple input) {
        OccurrenceEvent event = (OccurrenceEvent) input.getObjectByField("event");
        //System.out.println("Received Event of type:" + event.getType() + " value:" + event.getValue()); // debug
        boolean []matches;// = new boolean[db.getAllXPaths().keySet().size()];
        
        matches = xpathEngine.evaluateEvent(event);
        
        for(int i=0; i<matches.length; i++) {
            if(matches[i]) {
                db.addMatch(channelNames[i], event.getDocId());
            }
        }
        
//        if(event.getDepth() == 0 && event.getType() == OccurrenceEvent.EventType.ElementClose) {
//            master.incCount();
//        }
    }

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
    	//System.out.println("WordCount executor " + getExecutorId() + " has words: " + wordCounter.keySet());

    	//wordCounter.clear();
    }

    /**
     * Lets the downstream operators know our schema
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    /**
     * Used for debug purposes, shows our exeuctor/operator's unique ID
     */
	@Override
	public String getExecutorId() {
		return executorId;
	}

	/**
	 * Called during topology setup, sets the router to the next
	 * bolt
	 */
	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}

	/**
	 * The fields (schema) of our output stream
	 */
	@Override
	public Fields getSchema() {
		return schema;
	}
	
}
