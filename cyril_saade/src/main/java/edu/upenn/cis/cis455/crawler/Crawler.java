package edu.upenn.cis.cis455.crawler;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.LocalCluster;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.bolt.DocumentFetcherBolt;
import edu.upenn.cis.stormlite.bolt.FilterBolt;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.LinkExtractorBolt;
import edu.upenn.cis.stormlite.bolt.ParserBolt;
import edu.upenn.cis.stormlite.bolt.PathMatcherBolt;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.URLSpout;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.net.ssl.HttpsURLConnection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis.cis455.crawler.info.URLInfo;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.storage.StorageInterface;

import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.rmi.UnknownHostException;
import java.security.MessageDigest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.stormlite.CrawlerFactory;

public class Crawler implements CrawlMaster {
    static final int NUM_WORKERS = 10;
    static final Logger logger = LogManager.getLogger(Crawler.class);
    
    private static final String URL_SPOUT = "URL_SPOUT";
    private static final String FETCHER_BOLT = "FETCHER_BOLT";
    private static final String EXTRACTOR_BOLT = "EXTRACTOR_BOLT";
    private static final String FILTER_BOLT = "FILTER_BOLT";
    private static final String PARSER_BOLT = "PARSER_BOLT";
    private static final String PATH_MATCHER_BOLT = "PATH_MATCHER_BOLT";
    
    final String startUrl;
    final StorageInterface db;
    final int size;
    int count;
    int crawled = 0;
    int shutdown = 0;
    int busy = 0;
    
    Map<String, RobotsTxtInfo> robots = new HashMap<>();
    //List<CrawlWorker> workers = new ArrayList<>();
    
    BlockingQueue<String> siteQueue = new LinkedBlockingQueue<>();
    Map<String,List<URLInfo>> urlQueue = new ConcurrentHashMap<String, List<URLInfo>>();
    // Last-crawled info for delays
    Map<String, Long> lastCrawled = new HashMap<>();
    
    public Crawler(String startUrl, StorageInterface db, int size, int count) {
        this.startUrl = startUrl;
        this.db = db;
        this.size = size;
        this.count = count;
    }
    
    public void start() {
        // Enqueue the first URL
        URLInfo info = new URLInfo(startUrl);
        
        urlQueue.put(info.getHostName(), new ArrayList<URLInfo>());
        urlQueue.get(info.getHostName()).add(info);
        siteQueue.add(info.getHostName());
        siteQueue.add("wikipedia.org");
        siteQueue.add("espn.com");
        siteQueue.add("imdb.com");
        siteQueue.add("nytimes.com");
        System.out.println("Crawling started");
    }
    
    public boolean deferCrawl(String site) {
    	
    	Long lastCrawlTime ;
    	synchronized(this.lastCrawled) {
    		lastCrawlTime = this.lastCrawled.get(site);
    		this.lastCrawled.notifyAll();
    	}
    	
    	RobotsTxtInfo robFile = null;
    	synchronized(this.robots) {
    		robFile = this.robots.get(site);
    		this.robots.notifyAll();
    	}
    	
        
        if(robFile == null) {
            return false; // no robot file associated with this site
        }
        
        Integer crawlDelay = robFile.getCrawlDelay("cis455crawler");
        if(crawlDelay == null) {
            crawlDelay = robFile.getCrawlDelay("*");
            if(crawlDelay == null) {
                return false; // no crawl delay associated with cis455Crawler
            }
        }

        // Check if we have reached the crawl delay
        if((Instant.now().toEpochMilli() - lastCrawlTime.longValue()) < crawlDelay.longValue()) {
            long sleepTime = crawlDelay.longValue() - (Instant.now().toEpochMilli() - lastCrawlTime.longValue());
        		try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        		return true;
        }
        else {
            return false;
        }
    }
    
    @Override
    public boolean isOKtoCrawl(URLInfo info) {
        
    	boolean haveRobots = false;
        // Read the Robots.txt file, and save it
        synchronized(robots) {
        	if (!robots.containsKey(info.getHostName()))
			{
        		haveRobots = false;
			}
        	else
        		haveRobots = true;
        	robots.notifyAll();
        }
    	if (!haveRobots)
        { // if we don't have robots.txt
            try {
                System.out.println("Reading robots file "+ info.toString());
                if(!readRobotsFile(info)) {
                    System.out.println("Cannot read robots.txt file"); //TODO
                    synchronized(this.robots)
                    {
                    	RobotsTxtInfo rob = new RobotsTxtInfo();
                    	this.robots.put(info.getHostName(), rob);
                    	robots.notifyAll();
                    }
                }
                
            } catch (Exception e) {
                e.printStackTrace();
            }
            info.setNextOperation("Robot.txt");
//            return true;
        }

        // checks if it is ok to crawl based on directives inside robots.txt
        String []paths = info.getFilePath().split("/");
        String currPath = "";
        RobotsTxtInfo robFile;
        synchronized(robots) {
        	robFile = robots.get(info.getHostName());
	        robots.notifyAll();
        }
        
        if(robFile == null) {
        		return true;
        }
        for(int i=1; i<paths.length; i++) {
            boolean forbidden = false;
            currPath += "/" + paths[i];
            // checks for /path
            if(robFile.getDisallowedLinks("cis455crawler") != null) {
                if(robFile.getDisallowedLinks("cis455crawler").contains(currPath))
                    forbidden = true;
                if(robFile.getDisallowedLinks("cis455crawler").contains(currPath + "/"))
                    forbidden = true;
            }
            if (forbidden == false) {
	            if(robFile.getDisallowedLinks("*") != null) {
	                if(robFile.getDisallowedLinks("*").contains(currPath))
	                    forbidden = true;
	                if(robFile.getDisallowedLinks("*").contains(currPath + "/"))
	                    forbidden = true;
	            }
            }
            
            
            if(forbidden) {
                info.setNextOperation("NULL");
                return false;
            }
        }
        
        if(info.getNextOperation().equals("Robot.txt")) {
            info.setNextOperation("HEAD");
        }
        return true;
    }

    @Override
    public boolean isOKtoParse(URLInfo url) {
        if(url.isHTML() && url.getNextOperation().equals("NULL")) {
            return true;
        }
        return false;
    }

    @Override
    public synchronized void incCount() {
        //System.out.print(".");
        crawled++;
      //  System.out.println("Num indexed:" + crawled);
	    logger.info("Num indexed:"+crawled);
    }

    @Override
    public boolean isIndexable(String content) {
        return true;
    }
    
    @Override
    public boolean isDone() {
        return crawled >= count || (busy == 0 && siteQueue.isEmpty());
    }
    
    @Override
    public void notifyThreadExited() {
        shutdown++;
    }
    
    /**
     * Busy wait for shutdown
     */
    public void waitForThreadsToEnd() {
       /* while (shutdown < workers.size()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }*/
        System.out.println("Indexed:" + crawled + " documents");
        logger.info("Indexed:" + crawled + " documents");
        System.out.println("Threads are all shut down");
    }
    
    public void close() {
        db.close();
    }
    
    /*
     * Helper function to get the robots.txt file from a website
     * Submits a GET /robots.txt to the website
     * Parses the body (i.e. the file) and saves it into a map
     */
    public boolean readRobotsFile(URLInfo info) throws Exception{
        // Submit GET Req to site to get the Robots.txt file
        try {

            BufferedReader is;
            HttpsURLConnection connSec = null; // for Https
            HttpURLConnection conn = null; // for Http
            URL url = new URL(((info.isSecure())?"https://":"http://") + info.getHostName() + "/robots.txt"); // for Https

            logger.info("Downloading " + url.toString());
            
            if(info.isSecure()) {
                
                connSec = (HttpsURLConnection)url.openConnection();
                
                connSec.setRequestMethod("GET");
                connSec.setRequestProperty("User-Agent", "cis455Crawler");
                connSec.setRequestProperty("Host", info.getHostName());
                connSec.setConnectTimeout(1000);
                is = new BufferedReader(new InputStreamReader(connSec.getInputStream()));
            }
            else {
            		conn = (HttpURLConnection)url.openConnection();
            		conn.setRequestMethod("GET");
            		conn.setConnectTimeout(1000);
            		conn.setRequestProperty("User-Agent", "cis455Crawler");
            		conn.setRequestProperty("Host", info.getHostName());
            		
                is = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            }
            
            String currLine;
            if(!info.isSecure()) {
                while((currLine = is.readLine()) != null) {
                    if(currLine.equals("")) { // header is done
                        break;
                    }
                }
            }

            //////// Parsing Robots.txt file /////////
            RobotsTxtInfo newRob = new RobotsTxtInfo();
            String userAgent = "";
            while((currLine = is.readLine()) != null) {

            		if(currLine.trim().startsWith("#")) {
                    continue;
                }

                if(currLine.equals("") || currLine.equals(" ")) {
                    userAgent = "";
                    continue;
                }
                String header = currLine.split(":")[0];
                String val = "";
                try {val = currLine.split(":")[1];}catch(java.lang.ArrayIndexOutOfBoundsException e) {continue;}
                
                if(header.toLowerCase().equals("disallow")) {
                    newRob.addDisallowedLink(userAgent, val.trim());
                }
                else if(header.toLowerCase().equals("user-agent")) {
                    userAgent = val.trim();
                }
                else if(header.toLowerCase().equals("crawl-delay")) {
                    newRob.addCrawlDelay(userAgent, Integer.parseInt(val.trim()));
                }
            }
            // adding RobotsTxt to map
            synchronized(this.robots)
            {
            	this.robots.put(info.getHostName(), newRob);
            	robots.notifyAll();
            }
            
            
            synchronized(this.lastCrawled)
            {
            	this.lastCrawled.put(info.getHostName(), new Long(Instant.now().toEpochMilli()));
            	this.lastCrawled.notifyAll();
            }
            
            
            if(info.isSecure()) {
                connSec.disconnect();
            }
            else {
                conn.disconnect();
            }
            is.close();
            info.setNextOperation("HEAD");
            return true;
        }
        catch(java.lang.ArrayIndexOutOfBoundsException e1) {
        		e1.printStackTrace();
        		return false;
        }
        catch(IOException e) {
            throw e;
        }        
    }
    
    /*
     * Fetch file from the host specified in URLInfo
     * If it's a head request, then we will need to check if the file that we have associated with the URL is in our corpus
     * If it's in our corpus, we need to check if it has been modified since its timestamp
     */
//    public void sendRequest(URLInfo info) throws Exception {
//        try {
//            // Get the date somewhere here
//            boolean doModifiedSince = false;
//            Long timeStamp = db.getDocumentTimeStamp(info.toString());
//            String dateStr = "";        
//            if((timeStamp != null) && info.getNextOperation().equals("HEAD")) {
//                doModifiedSince = true;
//                SimpleDateFormat date = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z");
//                date.setTimeZone(TimeZone.getTimeZone("GMT"));
//                dateStr = date.format(new Date(timeStamp.longValue())); // TODO: check if this is correct
//            }
//            
//            BufferedReader is = null;
//            HttpURLConnection conn = null; // For Http
//            HttpsURLConnection connSec = null; // for Https
//            URL url = new URL(((info.isSecure())?"https://":"http://") + info.getHostName() + info.getFilePath());
//                
//                /******* SENDING REQUEST TO WEBSITE ****/
//                if(info.isSecure()) {
//                    
//                    connSec = (HttpsURLConnection)url.openConnection();
//                    connSec.setRequestMethod(info.getNextOperation());
//                    connSec.setRequestProperty("User-Agent", "cis455crawler");
//                    connSec.setRequestProperty("Host", info.getHostName());
//                    
//                    if(doModifiedSince)
//                        connSec.setRequestProperty("If-Modified-Since", dateStr);
//                    
//                    is = new BufferedReader(new InputStreamReader(connSec.getInputStream()));
//                }
//                else {
//                		conn = (HttpURLConnection)url.openConnection();
//                		conn.setRequestMethod(info.getNextOperation());
//                		conn.setRequestProperty("User-Agent", "cis455crawler");
//                		conn.setRequestProperty("Host", info.getHostName());
//                		
//                		if(doModifiedSince)
//                			conn.setRequestProperty("If-Modified-Since", dateStr);
//                    
//                    is = new BufferedReader(new InputStreamReader(conn.getInputStream()));
//                }
//                
//    
//                
//                /****** READING HEAD RESPONSE FROM WEBSITE ******/
//               
//               Integer responseCode = null;
//               String contentType = null;
//               Integer contentLength = null;
//               int docLength = -1;
//               
//               if(info.isSecure()) {
//                   responseCode = connSec.getResponseCode();
//                   for(Map.Entry<String, List<String>> header : connSec.getHeaderFields().entrySet()) {
//                       if(header.getKey() == null) {
//                           continue;
//                       }
//                       if(header.getKey().toLowerCase().equals("content-type")) {
//                           contentType = header.getValue().get(0);
//                       }
//                       else if(header.getKey().toLowerCase().equals("content-length")) {
//                           contentLength = Integer.parseInt(header.getValue().get(0).trim()) / 1000000;
//                           docLength = Integer.parseInt(header.getValue().get(0).trim());
//                       }
//                   }
//               }
//               else {
//                   String firstLine = is.readLine();  // reading first line
//                   responseCode = Integer.parseInt(firstLine.split(" ")[1].trim());
//                   while((firstLine = is.readLine()) != null) {
//                       if(firstLine.equals("") || firstLine.equals(" ") || firstLine.equals("/r/n")) {
//                           break;
//                       }
//                       if(!firstLine.contains(":")) {
//                           continue;
//                       }
//                       String headerName = firstLine.split(":")[0];
//                       if(headerName == null) {
//                           continue;
//                       }
//
//                       String headerVal = firstLine.split(":")[1];
//                       
//                       if(headerName.toLowerCase().equals("content-type")) {
//                            contentType = headerVal;
//                       }
//                       else if(headerName.toLowerCase().equals("content-length")) {
//                           contentLength = Integer.parseInt(headerVal.trim()) / 1000000;
//                           docLength = Integer.parseInt(headerVal.trim());
//                       }
//                   }
//               }
//
//                
//               if(info.getNextOperation().equals("HEAD") && (responseCode == 200)) { // if it has been modified since
//                    
//                    // checking if file is acceptable
//                    if((!contentType.toLowerCase().contains("html") && !contentType.toLowerCase().contains("xml")) || (contentLength > this.size)) {
//                        info.setNextOperation("NULL");
//                    }
//                    else {
//                        info.setNextOperation("GET"); // we will get the file
//                    }
//                    
//                    if(contentType.toLowerCase().contains("html")) {
//                        info.isHTML(true);
//                    }
//                    if(info.isSecure()) {
//                        connSec.disconnect();
//                        is.close();
//                    }
//                    else {
//                        conn.disconnect();
//                        is.close();
//                    }
//                    return;
//               }
//               else if(info.getNextOperation().equals("HEAD") && (responseCode == 304)) { // if it has not been modified since
//                   info.setNextOperation("NULL"); // we won't traverse it again
//                   logger.info("Not Modified " + info.toString());
//                   if(info.toString().contains("html") || info.toString().endsWith("/")) {
//                        info.isHTML(true);
//                    }
//                    
//                   if(info.isSecure()) {
//                        connSec.disconnect();
//                        is.close();
//                    }
//                    else {
//                        conn.disconnect();
//                        is.close();
//                    }
//                   return;
//               }
//               else if(info.getNextOperation().equals("HEAD")) { // if we got another error from the server
//                   System.out.println("ERROR in head request for " + info.toString() + " Response Code:" + responseCode.toString());
//                   if(info.isSecure()) {
//                        connSec.disconnect();
//                        is.close();
//                    }
//                    else {
//                        conn.disconnect();
//                        is.close();
//                    }
//                    System.exit(1);
//                   return;
//               }
//               
//               /****** READING GET RESPONSE FROM WEBSITE ******/
//               if(info.getNextOperation().equals("GET")) {
//              
//                   // check status
//                   if(responseCode != 200) {
//                       System.out.println("ERROR in GET request for " + info.toString() + " Response Code:" + responseCode.toString());
//                       info.setNextOperation("NULL");
//                       if(info.isSecure()) {
//                            connSec.disconnect();
//                            is.close();
//                        }
//                        else {
//                            conn.disconnect();
//                            is.close();
//                        }
//                        System.exit(1);
//                       return;
//                   }
//                   
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
//                        System.exit(1);
//                       return;
//                   }
//                    
//                    
//                   // Read body into a String
//                   char []rawBody = new char[docLength];
//                   is.read(rawBody, 0, docLength);
//                   String responseBody = new String(rawBody);
//                   
//                   // Computing MD5 hash of message
//                   MessageDigest md = MessageDigest.getInstance("MD5");
//                   String hexResponse = new String(md.digest(responseBody.getBytes("UTF-8")), "UTF-8");
//                   
//                   if(db.hasSeen(hexResponse)) {
//                       info.setNextOperation("SEEN");
//                       if(info.isSecure()) {
//                            connSec.disconnect();
//                            is.close();
//                        }
//                        else {
//                            conn.disconnect();
//                            is.close();
//                        }
//                       return;
//                   }
//                   
//                   // Add it to corpus
//                   logger.info("Downloading " + info.toString());
//                   db.addDocument(info.toString(), responseBody);
//                   
//                   // Add hash of the document in db.contentSeen
//                   db.insertSeen(hexResponse);
//                   info.setNextOperation("NULL");
//               }
//               
//               if(info.isSecure()) {
//                    connSec.disconnect();
//                    is.close();
//                }
//                else {
//                    conn.disconnect();
//                    is.close();
//                }
//           }
//           catch(Exception e) {
//               throw e;
//           }
//
//        return;
//    }
    
    @Override
    public synchronized void setWorking(boolean working) {
        if (working)
            busy++;
        else
            busy--;
    }    

    public Long getLastCrawled(String site) {
        return this.lastCrawled.get(site);
    }
    
    public Map<String, Long> getLastCrawledQueue() {
    		return this.lastCrawled;
    }
    
    public Map<String, List<URLInfo>> getURLQueue() {
    		return this.urlQueue;
    }
    
    public BlockingQueue<String> getSiteQueue() {
    		return this.siteQueue;
    }
    
    public int getMaxDocSize()
    {
    	return this.size;
    }

    /**
     * Main program:  init database, start crawler, wait
     * for it to notify that it is done, then close.
     */
    public static void main(String args[]) {
        if (args.length < 3 || args.length > 5) {
            System.out.println("Usage: Crawler {start URL} {database environment path} {max doc size in MB} {number of files to index}");
            System.exit(1);
        }
        
        if (!Files.exists(Paths.get(args[1]))) {
            try {
                Files.createDirectory(Paths.get(args[1]));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
        System.out.println("Crawler starting");
        String startUrl = args[0];
        String envPath = args[1];
        Integer size = Integer.valueOf(args[2]);
        Integer count = args.length == 4 ? Integer.valueOf(args[3]) : 100;
        
        StorageInterface db = StorageFactory.getDatabaseInstance(envPath);
        CrawlerFactory.setDatabaseInstance(db);
        
        Crawler crawler = new Crawler(startUrl, db, size, count);
        
        CrawlerFactory.setCrawlMasterInstance(crawler);
        CrawlerFactory.setURLQueue(crawler.getURLQueue());
        CrawlerFactory.setSiteQueue(crawler.getSiteQueue());
        
        System.out.println("Starting crawl of " + count + " documents, starting at " + startUrl);
        crawler.start();
        
        /*** Bolt and Spout declaration here ***/
        IRichSpout urlSpout = new URLSpout();
        IRichBolt documentFetcher = new DocumentFetcherBolt();
        IRichBolt linkExtractor = new LinkExtractorBolt();
        IRichBolt filterBolt = new FilterBolt();
        
        IRichBolt parserBolt = new ParserBolt();
        IRichBolt pathMatcherBolt = new PathMatcherBolt();
        
        Config config = new Config();

        TopologyBuilder builder = new TopologyBuilder();
        
        /*** Set bolt and spouts here ***/
        // Only one source ("spout") for the words
        builder.setSpout(URL_SPOUT, urlSpout, 1);
        
        // Four parallel word counters, each of which gets specific words
        builder.setBolt(FETCHER_BOLT, documentFetcher, 5).shuffleGrouping(URL_SPOUT);//, new Fields("URL")); (fieldGrouping)
        builder.setBolt(EXTRACTOR_BOLT, linkExtractor, 5).shuffleGrouping(FETCHER_BOLT);
        builder.setBolt(FILTER_BOLT, filterBolt, 5).shuffleGrouping(EXTRACTOR_BOLT);
        
        // A single printer bolt (and officially we round-robin)
        //builder.setBolt(PRINT_BOLT, printer, 4).shuffleGrouping(COUNT_BOLT);
        //builder.setBolt(PARSER_BOLT, parserBolt, 1).shuffleGrouping(FETCHER_BOLT);
        //builder.setBolt(PATH_MATCHER_BOLT, pathMatcherBolt, 1).shuffleGrouping(PARSER_BOLT);

        /*** Create topology ***/
        LocalCluster cluster = new LocalCluster();
        //Topology topo = builder.createTopology();

//        ObjectMapper mapper = new ObjectMapper();
//		try {
//			String str = mapper.writeValueAsString(topo);
//			
//			System.out.println("The StormLite topology is:\n" + str);
//		} catch (JsonProcessingException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
        
        try {
            /*** Submit topology ***/
            cluster.submitTopology("engine", config, 
            		builder.createTopology());
            while(!crawler.isDone()) {
                Thread.sleep(10000);
            }
            cluster.killTopology("engine");
            cluster.shutdown();
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }

        
//        while (!crawler.isDone())
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
//            
//        crawler.waitForThreadsToEnd();
        System.out.println("Done crawling!");
        crawler.close();
        System.exit(0);
    }

}
