package edu.upenn.cis.stormlite.bolt;

import edu.upenn.cis.cis455.crawler.info.URLInfo;
import edu.upenn.cis.cis455.crawler.CrawlMaster;
import edu.upenn.cis.cis455.storage.StorageInterface;

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

import edu.upenn.cis.stormlite.CrawlerFactory;

/**
 * Bolt that will read a document from an input stream
 * Will parse the document (both XML and HTML)
 * And will output an occurance event each time we traverse
 */
public class DocumentFetcherBolt implements IRichBolt{
    
    static Logger log = LogManager.getLogger(DocumentFetcherBolt.class);
	
	Fields schema = new Fields("URL", "body");
    String executorId = UUID.randomUUID().toString();
    final CrawlMaster master;
    final StorageInterface db;
    
    private OutputCollector collector;
    
    public DocumentFetcherBolt() {
        this.master = CrawlerFactory.getCrawlMasterInstance();
        this.db = CrawlerFactory.getDatabaseInstance();
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
        //master.setWorking(true);
        URLInfo url = (URLInfo) input.getObjectByField("URL");
        log.debug(getExecutorId() + " received URL: " + url.toString());
        try {
            if(url.getNextOperation().equals("Robot.txt")) {
                master.readRobotsFile(url);
            }
            
            // TODO: wait delay
            if(url.getNextOperation().equals("HEAD")) {
                master.sendRequest(url);
            }
            
            // TODO: wait delay
            if(url.getNextOperation().equals("GET")) {
                master.sendRequest(url);
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        

        if(url.getNextOperation().equals("NULL")) { // indexed the document
            master.incCount();
            //collector.emit(new Values<Object>(db.getDocument(url.toString())));
            collector.emit(new Values<Object>(url, db.getDocument(url.toString())));
        }
        //master.setWorking(false);
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
