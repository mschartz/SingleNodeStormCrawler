package edu.upenn.cis.stormlite.bolt;

import edu.upenn.cis.cis455.crawler.info.URLInfo;
import edu.upenn.cis.cis455.crawler.CrawlMaster;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;

import java.util.concurrent.BlockingQueue;
import edu.upenn.cis.cis455.storage.StorageInterface;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;

import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.stormlite.CrawlerFactory;

import java.io.BufferedReader;
import java.io.StringReader;

/**
 * Bolt that will read a document through its input stream
 * If the document is in HTML, it will extract all of its URLs
 * Will output a stream of extracted URLs
 */
public class LinkExtractorBolt implements IRichBolt{
    
    static Logger log = LogManager.getLogger(LinkExtractorBolt.class);
	
	Fields schema = new Fields("URL"); // add an extra field called "body" because im also gonnabe ommiting document body
    String executorId = UUID.randomUUID().toString();
    private OutputCollector collector;
    final CrawlMaster master;
    StorageInterface db;
    
    public LinkExtractorBolt() {
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
    public void execute(Tuple input) { // TODO: we will actually be receiving the raw HTML
    //TODO: save document to DB
        URLInfo url = (URLInfo) input.getObjectByField("URL");  // this would be key
        log.debug(getExecutorId() + " received URL: " + url.toString());
        String body = input.getStringByField("body");
        //System.out.println("received " + url.toString());
        /*** PARSING NEEDS TO BE DONE IN LinkExtractor (MOVE ALL IF STATEMENT BELOW TO LinkExtractor) ***/
        if(master.isOKtoParse(url)) {
            log.info("Parsing " + url.toString());
            
            //BufferedReader bufReader = new BufferedReader(new StringReader(body));
            log.debug(body);
            Document doc = Jsoup.parse(body);
            Elements links = doc.select("a[href]");
            StringBuilder outlinks = new StringBuilder();

            for(Element link: links) {
            		//System.out.println("Extrated Link:" + link.attr("abs:href"));
                    outlinks.append(link.attr("abs:href"));
                    outlinks.append( "\t");
            		enqueueLink(url, link.attr("abs:href"));
            }
            if(this.db.getPageRankRecord(url.toString()) == null) {
                System.out.println("ADDING\n " + url.toString() + " " + outlinks.toString());
                this.db.addPageRankRecord(url.toString(), outlinks.toString());
            }
//            String line=null;
//            try {
//                while( (line=bufReader.readLine()) != null ) {
//                		addLinks(url, line);
//                }
//            }
//            catch(Exception e) {
//                e.printStackTrace();
//            }
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
	
    void addLinks(URLInfo info, String line) {
        String txt = line;//.toLowerCase();
        
        
        int href = txt.toLowerCase().indexOf("href");
        while (href >= 0 && txt.length() > 0 && href < txt.length()) {
            href += 4;
            
            boolean foundEquals = false;
            while (href < txt.length() &&
                (txt.charAt(href) == ' ' || txt.charAt(href) == '\t' || txt.charAt(href) == '=')
                ) {
                    foundEquals = (txt.charAt(href) == '=');
                href++;
            }

                
            if (foundEquals && 
            href >= 0 && href < txt.length()) {
                char quote = txt.charAt(href);

                // HREF
                if (quote == '\'' || quote == '\"') {
                    int end = txt.indexOf(quote, href+1);
                    if (end >= href) {
                        enqueueLink(info, txt.substring(href+1, end));
                    }
               }
               txt = txt.substring(href);
            } 
            href = txt.toLowerCase().indexOf("href");
        }
    }
    
    void addToQueue(String nextUrl) {
        
        URLInfo info = new URLInfo(nextUrl);
        collector.emit(new Values<Object>(info));
    }
    
    void enqueueLink(URLInfo info, String link) {
    		//System.out.println("Curr URL:" + info.toString() + " Next URL:" + link);
    		if(link.equals(""))
    			return;
        if (link.startsWith("/")) {
            String nextUrl = (info.isSecure() ? "https://" : "http://") +
                info.getHostName() + (info.getPortNo() == 80 ? "" : ":" + info.getPortNo()) +
                link;
                
            addToQueue(nextUrl);
        } else if (link.startsWith("http://") ||
            link.startsWith("https://")) {
                
            addToQueue(link);
        } else {
            String nextUrl = "";
            if (info.toString().endsWith("/"))
                nextUrl = info.toString() + link;
            else {
                nextUrl = info.toString().substring(0, info.toString().lastIndexOf('/')+1) + link;
            }
            addToQueue(nextUrl);
        }
    }
    

}

