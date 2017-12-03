package edu.upenn.cis.stormlite.bolt;

import edu.upenn.cis.cis455.crawler.info.URLInfo;
import edu.upenn.cis.cis455.crawler.CrawlMaster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import java.util.concurrent.BlockingQueue;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;

import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.stormlite.CrawlerFactory;

/**
 * Bolt that will read an ArrayList of URLs and push that to the site queue
 */
public class FilterBolt implements IRichBolt{
    
    static Logger log = LogManager.getLogger(FilterBolt.class);
	
	//Fields schema = new Fields("URL", "Filter");
	Fields schema = null;
    String executorId = UUID.randomUUID().toString();
    BlockingQueue<String> siteQueue;
    Map<String, List<URLInfo>> urlQueue;
    
    private OutputCollector collector;
    private CrawlMaster master;
    
    public FilterBolt() {
        this.siteQueue = CrawlerFactory.getSiteQueue();
        this.urlQueue = CrawlerFactory.getURLqueue();
        this.master = CrawlerFactory.getCrawlMasterInstance();
    }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * Process a tuple received from the stream, incrementing our
     * counter and outputting a result
     */
    @Override
    public void execute(Tuple input) {
        try {
            //List<URLInfo> newUrls = (List<URLInfo>) input.getObjectByField("URL");
//            URLInfo newURL = (URLInfo) input.getObjectByField("URL");
//            log.debug(getExecutorId() + " New URL: " + newURL.toString());
//            System.out.println("New URL:" + newURL.toString());
//            synchronized(siteQueue) {
//            	synchronized(urlQueue) {
//           urlQueue.get(newURL.getHostName()).add(newURL);
//           siteQueue.put(newURL.getHostName());
//           urlQueue.notifyAll();
//            }
//            	siteQueue.notifyAll();
//            }

            //for(int i=0; i< newUrls.size(); i++) {
            //    log.debug(getExecutorId() + " New URL: " + newUrls.get(i).toString());
            //    siteQueue.put(newUrls.get(i));
            //}
        }
        catch(Exception e) {
            e.printStackTrace();
        }
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
