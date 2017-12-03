package edu.upenn.cis.stormlite.spout;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.cis455.crawler.Crawler;
import static edu.upenn.cis.cis455.crawler.Crawler.*;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.storage.StorageInterface;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;

public class CrawlerSpout implements IRichSpout {
	static Logger log = LogManager.getLogger(CrawlerSpout.class);

	
	String executorId = UUID.randomUUID().toString();
	
	//Destination
	SpoutOutputCollector collector;
	final StorageInterface db;
	
	public CrawlerSpout()
	{
		log.debug("Starting spout");
		//this.db = db;
		this.db = StorageFactory.getDatabaseInstance(Crawler.envPath);
	}
	
	@Override
	public void open(Map<String, String> config, TopologyContext topo, SpoutOutputCollector collector) {
		System.out.println("Open spout"+collector);
		this.collector = collector;
		
	}
	
	@Override
	public void close() {
		//Graceful shutdown of spout
		
		
	}
	
	@Override
	//Will emit URLs
	public void nextTuple() {
		synchronized(Crawler.siteQueue)
		{
			synchronized(Crawler.urlQueue) {
				
				try {
					if(siteQueue != null && !siteQueue.isEmpty())
					{
						String site;
						
						site = siteQueue.take();
						
						if((site != null) && (!urlQueue.isEmpty())
								&& (urlQueue.containsKey(site)))
						{
							List<String> urls= urlQueue.get(site);
							Iterator<String> iter = urls.iterator();
							while (iter.hasNext()) {
							    String url = iter.next();
								System.out.println(getExecutorId() + " emitting " + url);
				    	        this.collector.emit(new Values<Object>(url));
							}
						}
						//siteQueue.add(site); //--> Add only after link extraction
		 			}
				} 
				catch (InterruptedException e) {
					e.printStackTrace();
				}
			
				Crawler.urlQueue.notifyAll();
			}
			Crawler.siteQueue.notifyAll();
		}
		Thread.yield();
	}
		
	
	
	@Override
	public String getExecutorId() {
		
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url"));
		
	}

	



	

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
		
	}
	
	
}
