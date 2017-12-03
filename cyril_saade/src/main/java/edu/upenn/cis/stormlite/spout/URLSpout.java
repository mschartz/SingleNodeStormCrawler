package edu.upenn.cis.stormlite.spout;

import java.util.Random;
import java.util.UUID;
import java.util.Map;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;

import java.util.concurrent.BlockingQueue;

import edu.upenn.cis.cis455.crawler.info.URLInfo;
import edu.upenn.cis.cis455.storage.StorageInterface;
import edu.upenn.cis.cis455.crawler.CrawlMaster;

import edu.upenn.cis.stormlite.CrawlerFactory;

/**
 * Spout that will send URL to its output stream once at a time from its queue
 */
public class URLSpout implements IRichSpout {
    static Logger log = LogManager.getLogger(URLSpout.class);
    
    String executorId = UUID.randomUUID().toString();

    /**
	 * The collector is the destination for tuples; you "emit" tuples there
	 */
	SpoutOutputCollector collector;
	
	BlockingQueue<String> siteQueue;
	Map<String, List<URLInfo>> urlQueue;
    final CrawlMaster master;
    final StorageInterface db;
    
    public URLSpout() {
        this.siteQueue = CrawlerFactory.getSiteQueue();
        this.urlQueue = CrawlerFactory.getURLqueue();
        this.db = CrawlerFactory.getDatabaseInstance();
        this.master = CrawlerFactory.getCrawlMasterInstance();
    }
    

	@SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        log.debug(getExecutorId() + " opening URL reader");
    }


	public void close() {
	    return;
	}
	
	/**
	 * When this method is called, Storm is requesting that the Spout emit 
	 * tuples to the output collector. This method should be non-blocking, 
	 * so if the Spout has no tuples to emit, this method should return. 
	 */
	public void nextTuple() {
	    do {
            try {
            	synchronized(siteQueue) {
                String site = siteQueue.take();
                
                if((site != null) && (!urlQueue.isEmpty()) && (urlQueue.containsKey(site))) {
                		master.setWorking(true);
                		synchronized(urlQueue) {
					List<URLInfo> urls= urlQueue.get(site);
					System.out.println("Spout is on site:" + site);
					Iterator<URLInfo> iter = urls.iterator();
					while (iter.hasNext()) {
					    URLInfo url = iter.next();
					    if(master.isOKtoCrawl(url)) {
					    		while(master.deferCrawl(url.getHostName())) {} // busy wait
					    }
						System.out.println(getExecutorId() + " emitting " + url);
		    	        		this.collector.emit(new Values<Object>(url));
		    	        		urls.remove(url);
					}
					
					if(urls.isEmpty())
						urlQueue.remove(site);
					urlQueue.notifyAll();
                		}
                		//CrawlerFactory.getURLqueue().notifyAll();
					master.setWorking(false);
				}
                siteQueue.notifyAll();
            	}
            	//CrawlerFactory.getSiteQueue().notifyAll();
               } catch (InterruptedException ie) {
                ie.printStackTrace();
               }
        } while (!master.isDone());
        
        master.notifyThreadExited();
	}

	@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("URL"));
    }


	@Override
	public String getExecutorId() {
		return executorId;
	}


	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}
}
