package edu.upenn.cis.cis455.crawler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.core.util.FileUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis.cis455.crawler.info.URLInfo;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.storage.StorageInterface;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.LocalCluster;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.bolt.DOMParserBolt;
import edu.upenn.cis.stormlite.bolt.DocumentFetcherBolt;
import edu.upenn.cis.stormlite.bolt.LinkExtractorBolt;
import edu.upenn.cis.stormlite.bolt.PathMatcherBolt;
import edu.upenn.cis.stormlite.spout.CrawlerSpout;
import edu.upenn.cis.stormlite.tuple.Fields;

public class Crawler {

	public static String envPath;
	
	static String startUrl;
	public static int size;
    static int count;
    static int crawled = 0;
    static int shutdown = 0;
    static int busy = 0;
    public static BlockingQueue<String> siteQueue = new LinkedBlockingQueue<>();
    public static Map<String,List<String>> urlQueue = new HashMap<>();
    // Last-crawled info for delays
    static Map<String,Integer> lastCrawled = new HashMap<>();
    //URLInfo object 
    static Map<String, URLInfo> UrlInfoMap = new HashMap<>();
    public static Map<String, RobotsTxtInfo> robots = new HashMap<>();
    
    
    private static final String CRAWLER_SPOUT = "CRAWLER_SPOUT";
    private static final String DOC_FETCH_BOLT = "DOC_FETCH_BOLT";
    private static final String LINK_EXC_BOLT = "LINK_EXC_BOLT";
    private static final String DOM_PARSER_BOLT = "DOM_PARSER_BOLT";
    private static final String PATH_MAT_BOLT = "PATH_MAT_BOLT";
    
	public static void main(String[] args) {
		//Arguments
		if (args.length < 3 || args.length > 5) {
            System.out.println("Usage: Crawler {start URL} {database environment path} {max doc size in MB} {number of files to index}");
            System.exit(1);
        }
		
		startUrl = args[0];
        envPath = args[1];
        size = Integer.valueOf(args[2]);
        count = args.length == 4 ? Integer.valueOf(args[3]) : 100;
        
        // Enqueue the first URL
        URLInfo info = new URLInfo(startUrl);
        info.size(size);
        UrlInfoMap.put(startUrl, info);
        urlQueue.put(info.getHostName(), new ArrayList<String>());
        urlQueue.get(info.getHostName()).add(startUrl);
        //isOKtoCrawl to be checked
        siteQueue.add(info.getHostName());
        
        if (!Files.exists(Paths.get(args[1]))) {
            try {
                Files.createDirectory(Paths.get(args[0]));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        StorageInterface db = StorageFactory.getDatabaseInstance(envPath);
        
        
        
        //Stormlite topology creation
        Config config = new Config();
//        config.registerSerialization(Values/.class);
        CrawlerSpout spout = new CrawlerSpout();
        DocumentFetcherBolt fetcherBolt = new DocumentFetcherBolt();
        LinkExtractorBolt extractorBolt = new LinkExtractorBolt();
//        FilterUrlBolt filerUrlBolt = new FilterUrlBolt();
        DOMParserBolt domBolt = new DOMParserBolt();
        PathMatcherBolt pathBolt = new PathMatcherBolt();
        
        TopologyBuilder builder = new TopologyBuilder();
        
     // Only one source ("spout") for the urls
        builder.setSpout(CRAWLER_SPOUT, spout, 1);
        
     // A doc fetcher bolt (and officially we round-robin)
        builder.setBolt(DOC_FETCH_BOLT, fetcherBolt, 10).shuffleGrouping(CRAWLER_SPOUT); 

        //lINK EXTRACTOR BOLT THAT EXTRACTS ALL LINKS FROM THE DOCUMENT
        builder.setBolt(LINK_EXC_BOLT, extractorBolt, 10).shuffleGrouping(DOC_FETCH_BOLT);
        
        //Dom parser bolt
        builder.setBolt(DOM_PARSER_BOLT, domBolt, 10).shuffleGrouping(DOC_FETCH_BOLT);
        
        //Path Matcher bolt
//        builder.setBolt(PATH_MAT_BOLT, pathBolt, 40).fieldsGrouping(DOM_PARSER_BOLT, new Fields("docId"));
        builder.setBolt(PATH_MAT_BOLT, pathBolt, 1).shuffleGrouping(DOM_PARSER_BOLT);
        
        LocalCluster cluster = new LocalCluster();
        Topology topo = builder.createTopology();

        ObjectMapper mapper = new ObjectMapper();
		try {
			String str = mapper.writeValueAsString(topo);
			
			System.out.println("The StormLite topology is:\n" + str);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		cluster.submitTopology("crawler", config, 
        		builder.createTopology());
        try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//        try {
//			Files.delete(Paths.get(envPath));
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//        cluster.killTopology("crawler");
//        cluster.shutdown();
//        System.exit(0);
        
	}

}
