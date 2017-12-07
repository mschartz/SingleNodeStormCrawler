package edu.upenn.cis.stormlite.spout;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.cis455.crawler.CrawlerFactory;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;

public class URLSpout implements IRichSpout {
	static Logger log = LogManager.getLogger(URLSpout.class);
	String executorId = UUID.randomUUID().toString();
	SpoutOutputCollector collector;
	
	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("URL"));
	}

	@Override
	public void open(Map<String, String> config, TopologyContext topo, SpoutOutputCollector collector) {
		this.collector = collector;		
	}

	@Override
	public void close() {
		return;
	}

	@Override
	public void nextTuple() {
		do {
			try {
				String site = CrawlerFactory.getSiteQueue().take();
				synchronized(CrawlerFactory.getURLqueue()) {
					if((site != null) && (!CrawlerFactory.getURLqueue().isEmpty()) && (CrawlerFactory.getURLqueue().containsKey(site))) {
						CrawlerFactory.getCrawlMasterInstance().setWorking(true);
						
						Iterator<String> iter = CrawlerFactory.getURLqueue().get(site).iterator();
						while(iter.hasNext()) {
							String url = iter.next();
							if(CrawlerFactory.getCrawlMasterInstance().isOKtoCrawl(url)) {
								while(CrawlerFactory.getCrawlMasterInstance().deferCrawl(site)) {} // busy wait
									this.collector.emit(new Values<Object>(url));
							}
						}
					}
				}
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		} while(!CrawlerFactory.getCrawlMasterInstance().isDone());
		
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}

}
