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

import org.jsoup.Jsoup;
import org.jsoup.parser.Parser;
import org.jsoup.helper.Validate;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import edu.upenn.cis.cis455.model.OccurrenceEvent;

import edu.upenn.cis.cis455.crawler.CrawlMaster;
import edu.upenn.cis.stormlite.CrawlerFactory;

/**
 * Bolt that will read a document from an input stream
 * Will parse the document (both XML and HTML)
 * And will output an occurance event each time we traverse
 */
public class ParserBolt implements IRichBolt{
    
    static Logger log = LogManager.getLogger(ParserBolt.class);
	
	Fields schema = new Fields("event");
    String executorId = UUID.randomUUID().toString();
    private OutputCollector collector;
    private CrawlMaster master;
    
    public ParserBolt() {
        
        master = CrawlerFactory.getCrawlMasterInstance();
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
    public void execute(Tuple input) { // TODO: we will actually be receiving the raw HTML
        URLInfo url = (URLInfo) input.getObjectByField("URL");
        //System.out.println("Parsing: " + url.toString());
        String html = input.getStringByField("body");
        
        Document doc;
        if(url.isHTML()) {
            doc = Jsoup.parse(html);
        }
        else {
            doc = Jsoup.parse(html, "", Parser.xmlParser());
        }
        
        loopThroughDOM(doc.children().first(), url.toString(), 0);
    }
    
    private void loopThroughDOM(Element elem, String url, int depth) {
        // send element open

        OccurrenceEvent eventFirst = new OccurrenceEvent(url, OccurrenceEvent.EventType.ElementOpen, elem.nodeName(), depth);
        //System.out.println("Emitting Event of type:" + eventFirst.getType() + " value:" + eventFirst.getValue());
        collector.emit(new Values<Object>(eventFirst));
        
        if(elem.ownText() != null && !elem.ownText().equals("")){
            OccurrenceEvent eventMiddle = new OccurrenceEvent(url, OccurrenceEvent.EventType.Text, elem.ownText(), depth+1);
            //System.out.println("Emitting Event of type:" + eventMiddle.getType() + " value:" + eventMiddle.getValue());
            collector.emit(new Values<Object>(eventMiddle));
        }
        
        // go to children
        for(Element child : elem.children()) {
            loopThroughDOM(child, url, depth+1);
        }
        
        // send element close
        OccurrenceEvent eventLast = new OccurrenceEvent(url, OccurrenceEvent.EventType.ElementClose, elem.ownText(), depth);
        //System.out.println("Emitting Event of type:" + eventLast.getType() + " value:" + eventLast.getValue());
        collector.emit(new Values<Object>(eventLast));
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
