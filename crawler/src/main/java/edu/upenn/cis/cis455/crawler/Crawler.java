package edu.upenn.cis.cis455.crawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import javax.net.ssl.HttpsURLConnection;
import javax.xml.bind.DatatypeConverter;

import edu.upenn.cis.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis.cis455.crawler.info.URLInfo;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.storage.StorageInterface;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;

public class Crawler implements CrawlMaster {
    final static Logger logger = LogManager.getLogger(Crawler.class);

    static final int NUM_WORKERS = 10;
    public static int DB_PAGE_COUNT = 0;
    
    final String startUrl;
    final StorageInterface db;
    final int size;
    int count;
    public int crawled = 0;
    int shutdown = 0;
    int busy = 0;
    
    Map<String, RobotsTxtInfo> robots = new HashMap<>();
    List<CrawlWorker> workers = new ArrayList<>();

    BlockingQueue<String> siteQueue = new LinkedBlockingQueue<>();
    Map<String,List<String>> urlQueue = new ConcurrentHashMap<>();

    private Set<String> contentSeen = Collections.synchronizedSet(new HashSet<String>());
    private Set<String> urlSeen = new HashSet<String>();

    //site robots site -> set of paths to disallow for site

    public void addUrlSeen(String url) {this.urlSeen.add(url);}
    public synchronized void addContentSeen(String hash) {
   //     logger.debug("ADDING CONTENT SEEN NOW: "+ hash);
        contentSeen.add(hash);
    }

    public Set<String> getUrlsSeen() {
        return this.urlSeen;
    }
    // Last-crawled info for delays
    Map<String,Long> lastCrawled = new HashMap<>();
    
    public Crawler(String startUrl, StorageInterface db, int size, int count) {
        this.startUrl = startUrl;
        this.db = db;
        this.size = size;
        this.count = count;
        
        CrawlerFactory.setCrawlMasterInstance(this);
        CrawlerFactory.setDatabaseInstance(db);
        CrawlerFactory.setSiteQueue(siteQueue);
        CrawlerFactory.setURLQueue(urlQueue);
    }
    
    public void start() {
        // Enqueue the first URL
        URLInfo info = new URLInfo(startUrl);
        urlQueue.put(info.getHostName(), new ArrayList<String>());
        urlQueue.get(info.getHostName()).add(startUrl);

        // Failing here, what do i need to implement?
//        logger.debug("crawler info");
//        logger.debug("info: " + info.getHostName());
//        logger.debug("url: " + startUrl);
        siteQueue.add(startUrl);
        
        // Launch 10 workers
        for (int i = 0; i < NUM_WORKERS; i++) {
            CrawlWorker worker = new CrawlWorker(db, siteQueue, urlQueue, this);
            worker.start();
        }
        System.out.println("Crawling started");
    }
    
    public boolean deferCrawl(String site) {
        if (lastCrawled.get(site) != null)
            return (new java.util.Date().getTime() - lastCrawled.get(site)) < robots.get(site).getCrawlDelay(site);
        return false;
    }
    
    @Override
    public boolean isOKtoCrawl(String url) {
        URLInfo info = new URLInfo(url);
        String site = info.getHostName();
    		if (!robots.containsKey(site)) {
            try {
            		if(!readRobotsFile(url)) {
            			System.out.println("Cannot read robots file for " + url);
            		}
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    		 // checks if it is ok to crawl based on directives inside robots.txt
        String []paths = info.getFilePath().split("/");
        String currPath = "";
        RobotsTxtInfo robFile;
        	robFile = robots.get(site);
        
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
                return false;
            }
        }
        
        return true;
    }

    public String getRedirectLink(String response) {
        // make new call to that location
        String newLocation = "";

        for(String line : response.split("\r\n"))
        {
            if (line.toLowerCase().startsWith("location")) {
    //            logger.debug("REDIRECTING >>> " + line.split(" ")[1]);
                newLocation = line.split(" ")[1];
                break;
            }
        }
        return newLocation;
    }
    
    /*
     * Helper function to get the robots.txt file from a website
     * Submits a GET /robots.txt to the website
     * Parses the body (i.e. the file) and saves it into a map
     */
    public boolean readRobotsFile(String url) throws Exception{
    		URLInfo info = new URLInfo(url);
        // Submit GET Req to site to get the Robots.txt file
        try {

            BufferedReader is;
            HttpsURLConnection connSec = null; // for Https
            HttpURLConnection conn = null; // for Http
            URL urlFetch = new URL(((info.isSecure())?"https://":"http://") + info.getHostName() + "/robots.txt"); // for Https

            logger.info("Downloading " + url.toString());
            
            if(info.isSecure()) {
                
                connSec = (HttpsURLConnection)urlFetch.openConnection();
                
                connSec.setRequestMethod("GET");
                connSec.setRequestProperty("User-Agent", "cis455Crawler");
                connSec.setRequestProperty("Host", info.getHostName());
                
                is = new BufferedReader(new InputStreamReader(connSec.getInputStream()));
            }
            else {
            		conn = (HttpURLConnection)urlFetch.openConnection();
            		conn.setRequestMethod("GET");
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
                String val = currLine.split(":")[1];
                
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
            	this.robots.put(info.getHostName(), newRob);
            	robots.notifyAll();
            
            	this.lastCrawled.put(info.getHostName(), new Long(Instant.now().toEpochMilli()));
            	this.lastCrawled.notifyAll();
            
            if(info.isSecure()) {
                connSec.disconnect();
            }
            else {
                conn.disconnect();
            }
            is.close();
            return true;
        }
        catch(IOException e) {
            throw e;
        }        
    }


    @Override
    public boolean isOKtoParse(URLInfo url) {
        if(urlSeen.contains(url.toString()))
            return false;


        HttpClient client = new HttpClient();
        String contentType = "";
        int contentLength = 0;
        // Send HEAD before GET to gather info to make sure
        // ok to parse/crawl
        String response = client.sendRequest(url, "HEAD");

        if (response.toLowerCase().startsWith("http/1.1 30")) {
            String newLocation = getRedirectLink(response);

            HttpClient redirectClient = new HttpClient();
            URLInfo redirectUrlInfo = new URLInfo(newLocation);

            synchronized (this){
            response = redirectClient.sendRequest(redirectUrlInfo, "GET");
            }
        }

        // Check again for redirect response
        if (response.toLowerCase().startsWith("http/1.1 4"))
            return false;

        if (response.toLowerCase().startsWith("http/1.1 5"))
            return false;

        // finally, if request ok get content-type, if html return true
        for(String line : response.split("\r\n")) {
            if (line.toLowerCase().startsWith("content-type")) {
   //             logger.debug("CONTENT-TYPE === " + line.split(" ")[1]);
                contentType = line.split(" ")[1];
            }
            if(line.toLowerCase().startsWith("content-length")){
    ///            logger.debug("CONTENT-LENGTH: " + line.split(" ")[1]);
                contentLength = Integer.parseInt(line.split(" ")[1]);
            }
        }
  //      logger.debug("SIZES: " + contentLength + " " + this.size);
        if(contentLength > this.size * 1000000) // * 1000000 1MB == 1000000 bytes
            return false;

 //       logger.debug("IN IS ok TO PARSE " + url.getFilePath());

//        if (url.getFilePath().toLowerCase().contains("xml"))
//            return false;

        return true;
//        return (url.getFilePath().toLowerCase().endsWith("html") || contentType.toLowerCase().contains("html"));
    }

    @Override
    public void incCount() {
        System.out.print(".");
        crawled++;
    }

    @Override
     public boolean isIndexable(String content) {
        synchronized (contentSeen) {
//            logger.debug("ALL hashes in contentSeen: ");
            for (String s : contentSeen)
//                logger.debug(s);
//            logger.debug("content seen size()f is : " + contentSeen.size());
            if (contentSeen.contains(content))
                return false;
//            logger.debug("CONTENT NOT SEEN BEFORE: " + content);
            return true;
        }
    }
    
    @Override
    public synchronized boolean isDone() {
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
        while (shutdown < workers.size()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Threads are all shut down");
    }
    
    public void close() {
        db.close();
    }
    
    @Override
    public void setWorking(boolean working) {
        if (working) {
                busy++;
                logger.debug("INCREMENTED BUSY");
        }
        else{
                busy--;
                logger.debug("decremented BUSY");
        }
    }



    /**
     * Main program:  init database, start crawler, wait
     * for it to notify that it is done, then close.
     */
    public static void main(String args[]) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn.cis.cis455", Level.DEBUG);

        if (args.length < 3 || args.length > 5) {
            System.out.println("Usage: Crawler {start URL} {database environment path} {max doc size in MB} {number of files to index}");
            System.exit(1);
        }
        
        System.out.println("Crawler starting");
        String startUrl = args[0];
        String envPath = args[1];
        Integer size = Integer.valueOf(args[2]);
        Integer count = args.length == 4 ? Integer.valueOf(args[3]) : 100;
        
        StorageInterface db = StorageFactory.getDatabaseInstance(envPath);
        
        Crawler crawler = new Crawler(startUrl, db, size, count);
        
        System.out.println("Starting crawl of " + count + " documents, starting at " + startUrl);

        crawler.start();
        
        while (!crawler.isDone())
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            logger.debug("DB PAGE COUNT: " + DB_PAGE_COUNT);
        crawler.waitForThreadsToEnd();
        try {
            crawler.close();
        }
        catch(Exception e) {}
        // Need to close gracefully
        System.out.println("Done crawling!");
        logger.debug("DB PAGE COUNT: " + DB_PAGE_COUNT);

    }

    public String hash(String s){
        byte[] result = {};
        String strHash = "hash";
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            result = sha.digest(s.getBytes(StandardCharsets.UTF_8));
            strHash = DatatypeConverter.printHexBinary(result);
            System.out.println(strHash);
        }
        catch(java.security.NoSuchAlgorithmException e) {
            System.out.println("no such hash algorithmn");
        }
        return strHash;
    }

}
