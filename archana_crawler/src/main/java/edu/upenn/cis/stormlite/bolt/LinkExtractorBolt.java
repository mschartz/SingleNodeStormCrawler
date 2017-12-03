package edu.upenn.cis.stormlite.bolt;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;

import javax.net.ssl.HttpsURLConnection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.cis455.crawler.Crawler;
import edu.upenn.cis.cis455.crawler.info.URLInfo;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.storage.StorageInterface;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;

public class LinkExtractorBolt  implements IRichBolt {
	static Logger log = LogManager.getLogger(LinkExtractorBolt.class);

	String executorId = UUID.randomUUID().toString();
	Fields myFields = new Fields();
	
	OutputCollector collector;
	
	final StorageInterface db;
	
	public LinkExtractorBolt
	() { 
		
		System.out.println("creating link extractor bolt");
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
		System.out.println("Link ex "+getExecutorId() + "url "+ input.getStringByField("url"));
		System.out.println("Link ex "+getExecutorId() + "docId "+ input.getStringByField("docId"));
		System.out.println("Link ex "+getExecutorId() + "toParse "+ input.getStringByField("toParse"));
		
		String url = input.getStringByField("url");
		URLInfo urlObj = new URLInfo(url);
		
		if(input.getStringByField("toParse").equals("true"))
		{
			parseUrl(urlObj);
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
	
	public void parseUrl(URLInfo info) {
        System.out.println("Parsing " + info.toString());

        try {
            String files = db.getDocument(info.getHostName()+"/"+info.getFilePath());
            Scanner scanner = new Scanner(files);
            while (scanner.hasNextLine()) {
              String line = scanner.nextLine();
              StringBuilder response = new StringBuilder();
              
//                response.append(line);
                addLinks(info, line);
//                response.append('\n');
             
            }
            scanner.close();
            
            //indexText(response.toString());
        } catch (Exception mfe) {
            mfe.printStackTrace();
        } 

    }
	
	void addLinks(URLInfo info, String line) {
        String txt = line;//.toLowerCase();
        
//        System.out.println("Match " + txt);
        
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
	
	void enqueueLink(URLInfo info, String link) {
		// System.out.println("HREF: " + link + " Hostname:" +
		// info.getHostName());
		if (link.startsWith("/")) {
			String nextUrl = (info.isSecure() ? "https://" : "http://") + info.getHostName()
					+ (info.getPortNo() == 80 ? "" : ":" + info.getPortNo()) + link;

			addToQueue(nextUrl);
		} else if (link.startsWith("http://") || link.startsWith("https://")) {

			if (link.contains(info.getHostName())) {
				addToQueue(link);
			} else {
				addToQueue(link);
			}
		} else {
			String nextUrl = "";
			// if (info.toString().endsWith("/"))
			// nextUrl = info.toString() + link;
			// else {
			// nextUrl = info.toString().substring(0,
			// info.toString().lastIndexOf('/')+1) + link;
			// }
			nextUrl += ((info.isSecure() ? "https://" : "http://") + info.getHostName()
					+ (info.getPortNo() == 80 ? "" : ":" + info.getPortNo()) + info.getFilePath());
			if (nextUrl.endsWith("/"))
				nextUrl += link;
			else
				nextUrl = (nextUrl.substring(0, nextUrl.lastIndexOf('/') + 1) + link);
			addToQueue(nextUrl);
		}
	}
	
	synchronized void addToQueue(String nextUrl) {
        System.out.println("Next URL: " + nextUrl);
        
        URLInfo info = new URLInfo(nextUrl);
        
        if (!Crawler.urlQueue.containsKey(info.getHostName()))
        	Crawler.urlQueue.put(info.getHostName(), new ArrayList<>());
            
        Crawler.urlQueue.get(info.getHostName()).add(nextUrl);
        Crawler.siteQueue.add(info.getHostName());
    }
}
