package edu.upenn.cis.stormlite.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.cis455.crawler.Crawler;
import edu.upenn.cis.cis455.model.OccurrenceEvent;
import edu.upenn.cis.cis455.model.OccurrenceEvent.EventType;
import edu.upenn.cis.cis455.storage.DBWrapper;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.storage.StorageInterface;
import edu.upenn.cis.cis455.xpathengine.XPathEngineImpl;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;

import static edu.upenn.cis.cis455.xpathengine.XPathEngineFactory.*;

public class PathMatcherBolt implements IRichBolt {
	static Logger log = LogManager.getLogger(PathMatcherBolt.class);

	String executorId = UUID.randomUUID().toString();
	Fields myFields = new Fields();
	
	OutputCollector collector;
	
	final StorageInterface db;
	
	public PathMatcherBolt
	() { 
		
		System.out.println("creating path matcher bolt");
		this.db = StorageFactory.getDatabaseInstance(Crawler.envPath);
		
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("urls"));
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		System.out.println("Path m "+getExecutorId() + "docId "+ input.getStringByField("docId"));
		System.out.println("Path m "+getExecutorId() + "event "+ input.getStringByField("event"));
		System.out.println("Path m "+getExecutorId() + "name "+ input.getStringByField("name"));
		XPathEngineImpl pathMatcher = (XPathEngineImpl) getXPathEngine();
		String docId = input.getStringByField("docId");
		String url = input.getStringByField("url");
		//Read the xpaths from the db
		Set<String> channels = ((DBWrapper) db).getChannelsFromXPaths();
		String[] xpaths = new String[100];
		int i =0;
		List<String> xpa = new ArrayList<String>();
		if(channels != null)
		{
			for( String chan : channels)
			{
				if(chan!= null)
				{
					xpaths[i] = ((DBWrapper) db).getXpath(chan);
					xpa.add(xpaths[i]);
					i++;
				}
			}
		}
		pathMatcher.setXPaths(xpaths);
		
		//validate xpaths
		for(int j=0; j<i;j++)
		{
			if(!pathMatcher.isValid(j))
			{
				xpa.remove(j);
			}
		}
		
		pathMatcher.addFinalList(xpa);
		EventType ev = null;
		if(input.getStringByField("event").equals("ElementOpen)"))
			ev = EventType.ElementOpen;
		else if(input.getStringByField("event").equals("ElementClose)"))
			ev = EventType.ElementClose;
		else if(input.getStringByField("event").equals("Text)"))
			ev = EventType.Text;
		//Call evaluate
		if(ev!=null) {
			OccurrenceEvent oc = new OccurrenceEvent(input.getStringByField("docId"), ev, input.getStringByField("name"));
			pathMatcher.evaluateEvent(oc);
		}
		if(input.getStringByField("event").equals("DocumentClosed)"))
		{
			boolean[] pathsMatched = pathMatcher.getBoolArray();
			//Store the document to berkely db if matched
			if(channels != null)
			{
				int in = 0;
				for( String chan : channels)
				{
					if(pathsMatched[in] == true)
					{
						//Fetch the document using the docId
						String content = ((DBWrapper) db).getDocumentByDocId(Integer.valueOf(docId));
						((DBWrapper) db).addMatchedDocs(chan,url,content);
					}
				}
			}
			
		}
		
		
		
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
		
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}
}
