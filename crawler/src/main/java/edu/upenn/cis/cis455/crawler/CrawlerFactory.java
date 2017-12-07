package edu.upenn.cis.cis455.crawler;

import edu.upenn.cis.cis455.crawler.CrawlMaster;
import edu.upenn.cis.cis455.storage.StorageInterface;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import edu.upenn.cis.cis455.crawler.info.URLInfo;

public class CrawlerFactory {
    
    public static BlockingQueue<String> getSiteQueue() {
        return siteQueue;
    }
    
    public static Map<String, List<String>> getURLqueue() {
    		return urlQueue;
    }
    
    public static void setSiteQueue(BlockingQueue<String> queue) {
        siteQueue = queue;
    }
    
    public static void setURLQueue(Map<String, List<String>> map) {
    		urlQueue = map;
    }
    
    public static StorageInterface getDatabaseInstance() {
        return db;
    }
    
    public static void setDatabaseInstance(StorageInterface db_instance) {
        db = db_instance;
    }
    
    public static CrawlMaster getCrawlMasterInstance() {
        return crawler;
    }
    
    public static void setCrawlMasterInstance(CrawlMaster crawler_instance) {
        crawler = crawler_instance;
    }
    
    private static BlockingQueue<String> siteQueue;
    private static Map<String, List<String>> urlQueue;
    
    private static CrawlMaster crawler;
    private static StorageInterface db;
}
