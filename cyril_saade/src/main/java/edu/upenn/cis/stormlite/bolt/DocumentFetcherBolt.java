package edu.upenn.cis.stormlite.bolt;

import edu.upenn.cis.cis455.crawler.info.URLInfo;
import edu.upenn.cis.cis455.crawler.CrawlMaster;
import edu.upenn.cis.cis455.crawler.Crawler;
import edu.upenn.cis.cis455.storage.StorageInterface;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import javax.net.ssl.HttpsURLConnection;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;

import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

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
//        log.debug(getExecutorId() + " received URL: " + url.toString());
        System.out.println(getExecutorId() + " Indexing:" + url.toString());
        try {
            if(url.getNextOperation().equals("Robot.txt")) {
                
            		master.readRobotsFile(url);
            }
            
            // TODO: wait delay
            master.deferCrawl(url.getHostName());
            if(url.getNextOperation().equals("HEAD")) {
                sendRequest(url);
            }
            
            // TODO: wait delay
            master.deferCrawl(url.getHostName());
            if(url.getNextOperation().equals("GET")) {
                sendRequest(url);
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        

        if(url.getNextOperation().equals("NULL")) { // indexed the document
            master.incCount();
            collector.emit(new Values<Object>(url, db.getDocument(url.toString()))); //TODO: dont have time retrieving document from DB --> directly emit with body
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
	
	
    /*
     * Fetch file from the host specified in URLInfo
     * If it's a head request, then we will need to check if the file that we have associated with the URL is in our corpus
     * If it's in our corpus, we need to check if it has been modified since its timestamp
     */
    public void sendRequest(URLInfo info) throws Exception {
        try {
            // Get the date somewhere here
            boolean doModifiedSince = false;
            Long timeStamp = db.getDocumentTimeStamp(info.toString());
            String dateStr = "";        
            if((timeStamp != null) && info.getNextOperation().equals("HEAD")) {
                doModifiedSince = true;
                SimpleDateFormat date = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z");
                date.setTimeZone(TimeZone.getTimeZone("GMT"));
                dateStr = date.format(new Date(timeStamp.longValue())); // TODO: check if this is correct
            }
            else
            	doModifiedSince = false;
            
            BufferedReader is = null;
            HttpURLConnection conn = null; // For Http
            HttpsURLConnection connSec = null; // for Https
            URL url = new URL(((info.isSecure())?"https://":"http://") + info.getHostName() + info.getFilePath());
                
                /******* SENDING REQUEST TO WEBSITE ****/
                if(info.isSecure()) {
                    
                    connSec = (HttpsURLConnection)url.openConnection();
                    connSec.setRequestMethod(info.getNextOperation());
                    connSec.setRequestProperty("User-Agent", "cis455crawler");
                    connSec.setRequestProperty("Host", info.getHostName());
                    connSec.setDoOutput(false);
                    if(doModifiedSince)
                        connSec.setRequestProperty("If-Modified-Since", dateStr);
                    is = new BufferedReader(new InputStreamReader(connSec.getInputStream()));
                }
                else {
                		conn = (HttpURLConnection)url.openConnection();
                		conn.setRequestMethod(info.getNextOperation());
                		conn.setDoOutput(false);
                		conn.setRequestProperty("User-Agent", "cis455crawler");
                		conn.setRequestProperty("Host", info.getHostName());
                		
                		if(doModifiedSince)
                			conn.setRequestProperty("If-Modified-Since", dateStr);
                    is = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                }
                
    
                synchronized(master.getLastCrawledQueue())
                {
                		master.getLastCrawledQueue().put(info.getHostName(), new Long(Instant.now().toEpochMilli()));
                		master.getLastCrawledQueue().notifyAll();
                }
                
                /****** READING HEAD RESPONSE FROM WEBSITE ******/
               
               Integer responseCode = null;
               String contentType = null;
               Integer contentLength = null;
               int docLength = -1;
               
               if(info.isSecure()) {
                   responseCode = connSec.getResponseCode();
                   for(Map.Entry<String, List<String>> header : connSec.getHeaderFields().entrySet()) {
                       if(header.getKey() == null) {
                           continue;
                       }
                       if(header.getKey().toLowerCase().equals("content-type")) {
                           contentType = header.getValue().get(0);
                       }
                       else if(header.getKey().toLowerCase().equals("content-length")) {
                    	   
                           contentLength = Integer.parseInt(header.getValue().get(0).trim()) / 1000000;
                           docLength = Integer.parseInt(header.getValue().get(0).trim());
                       }
                   }
               }
               else {
            	   
                   responseCode = conn.getResponseCode();
                   for(Map.Entry<String, List<String>> header : conn.getHeaderFields().entrySet()) {
                       if(header.getKey() == null) {
                           continue;
                       }
                       if(header.getKey().toLowerCase().equals("content-type")) {
                           contentType = header.getValue().get(0);
                       }
                       else if(header.getKey().toLowerCase().equals("content-length")) {
                           contentLength = Integer.parseInt(header.getValue().get(0).trim()) / 1000000;
                           docLength = Integer.parseInt(header.getValue().get(0).trim());
                       }
                   }
               }

               if(contentLength == null) 
            	   		contentLength = 0;
               if(contentType == null) {
            	   		System.out.println("ERROR in HEAD: response does not contain content type - continue fetching");
            	   		contentType = "html";
               }
               if(info.getNextOperation().equals("HEAD") && (responseCode == 200)) { // if it has been modified since
                    // checking if file is acceptable
                    if((!contentType.toLowerCase().contains("html") 
                    		&& !contentType.toLowerCase().contains("xml")) 
                    		|| (contentLength > ((Crawler) master).getMaxDocSize())) {
                        info.setNextOperation("NULL");
                    }
                    else {
                        info.setNextOperation("GET"); // we will get the file
                    }
                    
                    if(contentType.toLowerCase().contains("html")) {
                        info.isHTML(true);
                    }
                    if(info.isSecure()) {
                        connSec.disconnect();
                        is.close();
                    }
                    else {
                        conn.disconnect();
                        is.close();
                    }
                    info.setNextOperation("GET");
                    return;
               }
               else if(info.getNextOperation().equals("HEAD") && (responseCode == 304)) { // if it has not been modified since
                   info.setNextOperation("NULL"); // we won't traverse it again
//                   logger.info("Not Modified " + info.toString());
                   if(info.toString().contains("html") || info.toString().endsWith("/")) {
                        info.isHTML(true);
                    }
                    
                   if(info.isSecure()) {
                        connSec.disconnect();
                        is.close();
                    }
                    else {
                        conn.disconnect();
                        is.close();
                    }
//                   info.setNextOperation("GET");
                   return;
               }
               else if(info.getNextOperation().equals("HEAD") && (responseCode == 302 || responseCode == 301)) { // redirect
            	   		URLInfo newUrl = null;
            	   		if(info.isSecure()) {
            	   			newUrl = new URLInfo(connSec.getHeaderFields().get("Location").get(0));
            	   		}
            	   		else {
            	   			newUrl = new URLInfo(conn.getHeaderFields().get("Location").get(0));
            	   		}
            	   		synchronized(CrawlerFactory.getURLqueue()) {
            	   			if(CrawlerFactory.getURLqueue().get(newUrl.getHostName()) == null || (CrawlerFactory.getURLqueue().get(newUrl.getHostName()).isEmpty()))
            	   				CrawlerFactory.getURLqueue().put(newUrl.getHostName(), new ArrayList<URLInfo>());
            	   			List<URLInfo> newUrls = CrawlerFactory.getURLqueue().get(newUrl.getHostName());
        	           
	        	           // checking if url exists if url list
	        	           boolean exists = false;
	        	           for(int i=0; i<newUrls.size(); i++) {
	        	        	   		if(newUrls.get(i).toString().equals(newUrl.toString())) {
	        	        	   			exists = true;
	        	        	   			break;
	        	        	   		}
	        	           }
        	           
	        	           if(!exists && !newUrls.contains(newUrl))
	        	        	   		newUrls.add(newUrl);
	        	           //CrawlerFactory.getURLqueue().put(newUrl.getHostName(), newUrls);
	        	          
	        	           CrawlerFactory.getURLqueue().notifyAll();   
            	   		}
            	   		synchronized (CrawlerFactory.getSiteQueue()) {
                            if (!CrawlerFactory.getSiteQueue().contains(newUrl.getHostName()))
                                CrawlerFactory.getSiteQueue().put(newUrl.getHostName());
                            CrawlerFactory.getSiteQueue().notifyAll();
                        }
            	   		if(info.isSecure()) {
                        connSec.disconnect();
                        is.close();
                    }
                    else {
                        conn.disconnect();
                        is.close();
                    }
            	   		return;
               }
               else if(info.getNextOperation().equals("HEAD")) { // if we got another error from the server
                   System.out.println("ERROR in head request for " + info.toString() + " Response Code:" + responseCode.toString());
                   if(info.isSecure()) {
                        connSec.disconnect();
                        is.close();
                    }
                    else {
                        conn.disconnect();
                        is.close();
                    }
                   // System.exit(1);
                   return;
               }
               
//               /****** SENDING GET RESPONSE FROM WEBSITE ******/
//               
//               if(info.isSecure()) {
//                   
//                   connSec = (HttpsURLConnection)url.openConnection();
//                   System.out.println("Connection Type: "+info.getNextOperation());
//                   connSec.setRequestMethod(info.getNextOperation());
//                   connSec.setRequestProperty("User-Agent", "cis455crawler");
//                   connSec.setRequestProperty("Host", info.getHostName());
//                   
//                   is = new BufferedReader(new InputStreamReader(connSec.getInputStream()));
//               }
//               else {
//               		conn = (HttpURLConnection)url.openConnection();
//               		System.out.println("Connection Type: "+info.getNextOperation());
//               		conn.setRequestMethod(info.getNextOperation());
//               		conn.setRequestProperty("User-Agent", "cis455crawler");
//               		conn.setRequestProperty("Host", info.getHostName());
//               		
//                   is = new BufferedReader(new InputStreamReader(conn.getInputStream()));
//               }
               
               /****** READING GET RESPONSE FROM WEBSITE ******/
               if(info.getNextOperation().equals("GET")) {
                   // check status
                   if(responseCode != 200) {
                       System.out.println("ERROR in GET request for " + info.toString() + " Response Code:" + responseCode.toString());
                       info.setNextOperation("NULL");
                       if(info.isSecure()) {
                            connSec.disconnect();
                            is.close();
                        }
                        else {
                            conn.disconnect();
                            is.close();
                        }
                        System.exit(1);
                       return;
                   }
                   
//                   if(docLength == -1) {
//                       System.out.println("ERROR in GET request for " + info.toString() + " Response header does not include content-length header");
//                       info.setNextOperation("NULL");
//
//                       if(info.isSecure()) {
//                            connSec.disconnect();
//                            is.close();
//                        }
//                        else {
//                            conn.disconnect();
//                            is.close();
//                        }
//                       // System.exit(1);
//                       return;
//                   }
                   
                   //while(!is.readLine().contains("/head")) {continue;} //skipping the head
                   // Read body into a String
                   String responseBody;
                   
                   try {
                	   		Document doc = Jsoup.connect(url.toString()).get();
                	   		responseBody = doc.toString();
                   }
                   catch(org.jsoup.UnsupportedMimeTypeException e) {
                	   		System.out.println("UNSUPPORTED MIME for URL" + url.toString());
                	   		info.setNextOperation("NULL");
                	   		return;
//                	   		while(!is.readLine().contains("/head")) {continue;} //skipping the head
//                	   		char []rawBody = new char[docLength];
//                         is.read(rawBody, 0, docLength);
//                         responseBody = new String(rawBody);
                	   		
                   }
                   //char []rawBody = new char[docLength];
                   //is.read(rawBody, 0, docLength);
                   //String responseBody = new String(rawBody);
                   
                   // Computing MD5 hash of message
                   MessageDigest md = MessageDigest.getInstance("MD5");
                   String hexResponse = new String(md.digest(responseBody.getBytes("UTF-8")), "UTF-8");
                   
                   if(db.hasSeen(hexResponse)) {
                       info.setNextOperation("SEEN");
                       if(info.isSecure()) {
                            connSec.disconnect();
                            is.close();
                        }
                        else {
                            conn.disconnect();
                            is.close();
                        }
                       return;
                   }
                   
                   // Add it to corpus
//                   logger.info("Downloading " + info.toString());
//                   System.out.println("File Stored: "+ responseBody);
                   db.addDocument(info.toString(), responseBody);
                   
                   // Add hash of the document in db.contentSeen
                   db.insertSeen(hexResponse);
                   info.setNextOperation("NULL");
               }
               
               if(info.isSecure()) {
                    connSec.disconnect();
                    is.close();
                }
                else {
                    conn.disconnect();
                    is.close();
                }
           }
           catch(Exception e) {
        	   	e.printStackTrace();   
        	   	throw e;
           }

        return;
    }
}
