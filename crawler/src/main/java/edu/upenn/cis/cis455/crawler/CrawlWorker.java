package edu.upenn.cis.cis455.crawler;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import edu.upenn.cis.cis455.crawler.info.URLInfo;
import edu.upenn.cis.cis455.storage.StorageInterface;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.net.ssl.HttpsURLConnection;
import javax.xml.bind.DatatypeConverter;


public class CrawlWorker extends Thread {
    final static Logger logger = LogManager.getLogger(CrawlWorker.class);

    BlockingQueue<String> siteQueue;
    Map<String,List<String>> urlQueue;
    final CrawlMaster master;
    final StorageInterface db;

    public CrawlWorker(StorageInterface db, BlockingQueue<String> queue, Map<String,List<String>> urlQueue, CrawlMaster master) {
        setDaemon(true);
        this.db = db;
        this.siteQueue = queue;
        this.urlQueue = urlQueue;
        this.master = master;
    }

    public void run() {
        do {
            try {

                 String siteUrl = siteQueue.take();

                  while (!urlQueue.isEmpty()) {

                            master.setWorking(true);

                            crawl(siteUrl, urlQueue, siteQueue);
                            urlQueue.remove(siteUrl);

                            master.setWorking(false);

                    }


            } catch (InterruptedException ie) {
                logger.debug("INTERUPTED EXCP");
            }


        } while (!master.isDone());

        master.notifyThreadExited();
    }


    public void crawl(String site, Map<String,List<String>> urlQueue, BlockingQueue<String> siteQueue) {
        URLInfo info = null;
        do {
            String host = site.startsWith("http://") ? site.substring(7) : site.substring(8);
            List<String> urlsFromSite = urlQueue.get(host);

            synchronized(this) {

                if (urlsFromSite != null && !urlsFromSite.isEmpty()) {
//                logger.debug("in crawl: " + urlsFromSite);

                    info = new URLInfo(urlsFromSite.remove(0));
                    if(urlsFromSite.isEmpty()) {
                        urlQueue.remove(host);
                        break;
                    }
                    if (master.isOKtoCrawl(site, info.getPortNo(), info.isSecure())) {

                        // If we need to defer the crawl, put the URL back in its list
                        // and move the site to the back of the crawl queue
                        if (master.deferCrawl(site)) {
                            urlsFromSite.add(0, info.toString());
                            if (!siteQueue.contains(site))
                                siteQueue.add(site);
                        } else if (master.isOKtoParse(info)) {

                            // Add back to the end of the queue
                            if (!urlsFromSite.isEmpty()) {

                                if (!siteQueue.contains(site))
                                    siteQueue.add(site);

                                // Nothing left from this site
                            } else {
                            }
                            break;
                        }
                    }

                }
                siteQueue.remove(site);
//                urlQueue.remove(host);

            }

        }  while (!master.isDone());

        if (info != null && master.isOKtoParse(info)) {
            // Parse
            parseUrl(info);
            master.addUrlSeen(info.toString());

        }

        master.incCount();
        //crawled++;
    }

    public void parseUrl(URLInfo info) {
        System.out.println("Parsing " + info.toString());

        try {
            URL url = new URL(info.toString());

            InputStream stream = null;

            if (info.isSecure()) {
                HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
                connection.setRequestProperty("User-Agent", "cis455crawler");
                stream = connection.getInputStream();
            } else {
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestProperty("User-Agent", "cis455crawler");
                stream = connection.getInputStream();
            }

            BufferedReader rd = new BufferedReader(new InputStreamReader(stream));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = rd.readLine()) != null) {
              response.append(line);
              addLinks(info, line);
              response.append('\n');
            }
            rd.close();

            // If hash(response.toString()) in contentSeen() don't index
            synchronized(master) {
                if (master.isIndexable(hash(response.toString()))) {
                    //logger.debug("INDEXING TEXT FOR URL: " + url.toString());
                    logger.debug("DB PAGE COUNT: " + Crawler.DB_PAGE_COUNT);
                    indexText(url.toString(), response.toString());
                }
            }
        } catch (Exception mfe) {
            mfe.printStackTrace();
        }

    }

    synchronized void addLinks(URLInfo info, String line) {
        String txt = line;//.toLowerCase();

     //   System.out.println("PATH: " + info.getFilePath());
       // System.out.println("Match " + txt);

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
                        if(!master.getUrlsSeen().contains(info.toString())) {
                            enqueueLink(info, txt.substring(href + 1, end));
                        }
                    }
               }
               txt = txt.substring(href);
            }
            href = txt.toLowerCase().indexOf("href");
        }
    }

    synchronized void addToQueue(String nextUrl) {

        System.out.println("Next URL: " + nextUrl);

        URLInfo info = new URLInfo(nextUrl);

        if (!urlQueue.containsKey(info.getHostName()))
            urlQueue.put(info.getHostName(), new ArrayList<>());

        if(urlQueue.get(info.getHostName()) != null) {
            if(!urlQueue.get(info.getHostName()).contains(info.toString()) && !master.getUrlsSeen().contains(nextUrl)) {

                urlQueue.get(info.getHostName()).add(nextUrl);

            }
        }
       // siteQueue.add(info.getHostName());
    }

   synchronized void enqueueLink(URLInfo info, String link) {
        System.out.println("HREF: " + link);
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

    void indexText(String url, String line) {
       // logger.debug("INDEX: " + line.hashCode() + "END INDEX");
        synchronized(master) {
 //           logger.debug("Indexing content at URL: " + url);
            master.addContentSeen(hash(line));
        }
        db.addDocument(url, line);

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
