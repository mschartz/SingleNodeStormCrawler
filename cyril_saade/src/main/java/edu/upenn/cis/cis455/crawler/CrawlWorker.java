package edu.upenn.cis.cis455.crawler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import edu.upenn.cis.cis455.crawler.info.URLInfo;
import edu.upenn.cis.cis455.storage.StorageInterface;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.HttpsURLConnection;

public class CrawlWorker extends Thread {
    final static Logger logger = LogManager.getLogger(CrawlWorker.class);
    BlockingQueue<URLInfo> siteQueue;
    //Map<String,List<URLInfo>> urlQueue;
    final CrawlMaster master;
    final StorageInterface db;
    
    public CrawlWorker(StorageInterface db, BlockingQueue<URLInfo> queue, CrawlMaster master) {
        setDaemon(true);
        this.db = db;
        this.siteQueue = queue;
        //this.urlQueue = urlQueue;
        this.master = master;
    }
    
    public void run() {
        do {
            try {
                URLInfo url = siteQueue.take();
                
                if (url != null) {
                    master.setWorking(true);
                    synchronized(siteQueue) {
                        crawl(url, siteQueue);
                    }

                    master.setWorking(false);
                }
            
            } catch (InterruptedException ie) {
                
            }

        } while (!master.isDone());
        
        master.notifyThreadExited();
    }
    
    
    public void crawl(URLInfo info, BlockingQueue<URLInfo> siteQueue) {
        String site = info.getHostName();
        try {
            //do {
                //List<URLInfo> urlsFromSite = urlQueue.get(site);
                //System.out.println("Working on: " + info.toString());
                //if (urlsFromSite != null && !urlsFromSite.isEmpty()) {
                    //info = urlsFromSite.remove(0);
                    if (master.isOKtoCrawl(info)) {
                        // If we need to defer the crawl, put the URL back in its list
                        // and move the site to the back of the crawl queue
                        if (master.deferCrawl(site)) {
                            //urlsFromSite.add(0, info);
                            //siteQueue.add(info);
                            addToQueue(info); // try to declare URLINfo with the same param of current urlinfo
                            return;
                        }
                        // we can get the file now
                        else {
                            // Submit HEAD/GET with document (with modified since header)
                            master.sendRequest(info);

                            if(!info.getNextOperation().equals("NULL") && !info.getNextOperation().equals("SEEN")) {
                                //urlsFromSite.add(0, info);
                                //siteQueue.add(info);
                                addToQueue(info); // try to declare new urlinfo with the same param of current urlinfo, and add new one to queue
                                return;
                            }
                            
                            // TODO: Store content-seen of document as MD5 hash
                            if(info.getNextOperation().equals("SEEN")) {
                                return; // redundant file: don't parse it
                            }

                            master.incCount(); // mark that we indexed another document
                            if (master.isOKtoParse(info)) { // if its ok to get the links from the file (true if file is html false otherwise)

                                logger.info("Parsing " + info.toString());
                                // First: we get the document from the corpus -> getDocument(url)
                                String html = this.db.getDocument(info.toString());
                                Document doc = Jsoup.parse(html);
                        
                                //(1): Extract all URLs in document (i.e. all hrefs) --> is this fetch?
                                Elements links = doc.select("a[href]");
                                for(Element link : links) {
                                    String href = link.attr("href");
                                    String absPath = this.findAbsoluteURL(info, href);
                                    URLInfo nextUrl = new URLInfo(absPath);
                                    addToQueue(nextUrl); // add all links to queue
                                }
                                // Add back to the end of the queue
                                //if (!urlsFromSite.isEmpty()) {
                                //    siteQueue.add(site);
                                // Nothing left from this site
                                //} else {
                                //    urlQueue.remove(site);
                                //}
        
                            }
                            //break;
                        }
    
    
        
                    }
                    else {
                        //System.out.println("NOT OK TO CRAWL");
                    }
                //}
            //}  while (!master.isDone()); // siteQueue.contains(site)
        }
        catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    public String findAbsoluteURL(URLInfo info, String href) {
        String url = "";
        href = href.replace("http://", "");
        href = href.replace("https://", "");
        href = href.replace("www.", "");
        
        String pathToFile = info.getFilePath().replace(".html","/");
        if(pathToFile.startsWith("/")) {
            pathToFile = pathToFile.substring(1, pathToFile.length());
        }


        if(href.startsWith("/")) { // relative to root
            url = info.getHostName() + href;
        }
        else if(href.startsWith("../")) {
            int numDirUp = href.split("../").length-1;
            for(int i=1; i<info.getFilePath().split("/").length-numDirUp; i++) {
                url = url + "/" + info.getFilePath().split("/")[i];
            }
            href = href.replace("../", "/");
            url = info.getHostName() + url + href;
        }
        else if(!href.startsWith(info.getHostName()) && !href.startsWith(pathToFile)) {
            String off = (!info.getFilePath().endsWith("/")?"/":"");
            url = info.getHostName() + info.getFilePath().replace(".html","") + off + href;
        }
        else {
            url = info.getHostName() + "/" + href;
        }

        return ((info.isSecure())?"https://":"http://") + url;
    }
    
    void addToQueue(URLInfo info) {
       // try {
        //System.out.println("Next URL: " + nextUrl);
        
        //URLInfo info = new URLInfo(nextUrl);
        
        //if (!urlQueue.containsKey(info.getHostName()))
         //   urlQueue.put(info.getHostName(), new ArrayList<>());

        //urlQueue.get(info.getHostName()).add(info);
        siteQueue.add(info);
    //}
    //catch(InterruptedException e) {
    //    e.printStackTrace();
    //    System.exit(1);
    //}

    }
}
