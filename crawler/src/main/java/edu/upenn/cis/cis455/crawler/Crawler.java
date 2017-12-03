package edu.upenn.cis.cis455.crawler;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.BlockingQueue;
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
    Map<String,List<String>> urlQueue = new HashMap<>();

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
    Map<String,Integer> lastCrawled = new HashMap<>();
    
    public Crawler(String startUrl, StorageInterface db, int size, int count) {
        this.startUrl = startUrl;
        this.db = db;
        this.size = size;
        this.count = count;
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
    public boolean isOKtoCrawl(String site, int port, boolean isSecure) {
        if (!robots.containsKey(site)) {
            try {
                System.out.println("Site: " + site);
                URL url = new URL(isSecure ? "https://" : "http://" + site + ":" + port+ "/robots.txt");

                if (isSecure) {
                    HttpsURLConnection conn = (HttpsURLConnection)url.openConnection();
                    conn.disconnect();
                } else {
                    HttpURLConnection conn = (HttpURLConnection)url.openConnection();
                    conn.disconnect();
                }

                RobotsTxtInfo robot = new RobotsTxtInfo();

                if (robots.get(startUrl) == null) {
                    robot = getRobotsInfoForSite();
                }

                robots.put(site, robot);

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        return robots.get(site) == null || (robots.get(site).getDisallowedLinks("*") == null ||
            !robots.get(site).getDisallowedLinks("*").contains("/")) ||
            (robots.get(site).getDisallowedLinks("cis455crawler") == null ||
            !robots.get(site).getDisallowedLinks("cis455crawler").contains("/"));
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

    public RobotsTxtInfo getRobotsInfoForSite() {
        RobotsTxtInfo info;
        HttpClient client = new HttpClient();
        URLInfo urlInfo = new URLInfo(startUrl + "/robots.txt");


        String responseText = client.sendRequest(urlInfo, "GET");

        // only handling 1-level redirection
        // TODO: should implement in more general way

        if (responseText.toLowerCase().startsWith("http/1.1 30")) {

            String newLocation = getRedirectLink(responseText);

            HttpClient redirectClient = new HttpClient();
            URLInfo redirectUrlInfo = new URLInfo(newLocation);

            String redirectResponseText = redirectClient.sendRequest(redirectUrlInfo, "GET");

            info = parseRobotsFile(redirectResponseText);
        }
        // Not a redirect
        else {
            // parse robots.txt response as usual
            info = parseRobotsFile(responseText);
        }

        // TODO: WHAT ABOUT 404s?

        return info;
    }

    public RobotsTxtInfo parseRobotsFile(String response)
    {
        RobotsTxtInfo robots = new RobotsTxtInfo();
        StringBuilder rulesBuilder;
        String currentUA = "";

        for (String line : response.split("\r\n")) {
            String lower = line.toLowerCase();
            //logger.debug("LINE: " + line + "|" + (lower.contains("cis455crawler")));
            try {
                if (lower.startsWith("user-agent:")) {
                    currentUA = line.split(" ")[1].trim();
    //                logger.debug("CURRENT USER-AGENT: " + currentUA);
                    robots.addUserAgent(currentUA);
                    continue;
                }

                // DISALLOW
                if (lower.startsWith("disallow:")) {
                    robots.addDisallowedLink(currentUA, line.split(" ")[1].trim());
                }
                // CRAWL-DELAY
                if (lower.startsWith("crawl-delay:")) {
                    robots.addCrawlDelay(currentUA, Integer.parseInt(line.split(" ")[1].trim()));
                }
                // ALLOW
                if (lower.startsWith("allow:")) {
                    robots.addAllowedLink(currentUA, line.split(" ")[1].trim());
                }
                // SITEMAP
                if (lower.startsWith("sitemap:")) {
                    robots.addSitemapLink(line.split(" ")[1].trim());
                }

            }
            catch(Exception e) {
                logger.debug("PARSER ERROR WHILE WORKING ON ROBOTS FILE: " + e);
            }
        }
        return robots;
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
