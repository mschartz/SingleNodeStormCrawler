package edu.upenn.cis.cis455.crawler;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.net.ssl.HttpsURLConnection;

import edu.upenn.cis.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis.cis455.crawler.info.URLInfo;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.storage.StorageInterface;


import edu.upenn.cis.cis455.xpathengine.*;
import static edu.upenn.cis.cis455.xpathengine.XPathEngineFactory.*;

public class Crawler_MS1 implements CrawlMaster {
    static final int NUM_WORKERS = 10;
    
    final String startUrl;
    final StorageInterface db;
    final int size;
    int count;
    int crawled = 0;
    int shutdown = 0;
    int busy = 0;
    URLInfo info;
    Map<String, RobotsTxtInfo> robots = new HashMap<>();
    List<CrawlWorker> workers = new ArrayList<>();
    
    BlockingQueue<String> siteQueue = new LinkedBlockingQueue<>();
    Map<String,List<String>> urlQueue = new HashMap<>();
    // Last-crawled info for delays
    Map<String,Integer> lastCrawled = new HashMap<>();
    
    public Crawler_MS1(String startUrl, StorageInterface db, int size, int count) {
        this.startUrl = startUrl;
        this.db = db;
        this.size = size;
        this.count = count;
    }
    
    public void start() {
        // Enqueue the first URL
        info = new URLInfo(startUrl);
        info.size(size);
        urlQueue.put(info.getHostName(), new ArrayList<String>());
        urlQueue.get(info.getHostName()).add(startUrl);
        //isOKtoCrawl to be checked
        siteQueue.add(info.getHostName());
        
        // Launch 10 workers
        for (int i = 0; i < NUM_WORKERS; i++) {
            CrawlWorker worker = new CrawlWorker(db, siteQueue, urlQueue, this);
            worker.start();
        }
        System.out.println("Crawling started");
    }
    
    public boolean deferCrawl(String site) {
       //Check for crawl delay from robots.txt
    	//TO DO: Change sleep to check time elapsed
    	if(robots.containsKey(site))
			try {
				if(robots.containsKey(site))
				{
					Thread.sleep(robots.get(site).getCrawlDelay(site));
				}
				else
				{
					if(robots.containsKey("*"))
					{
						Thread.sleep(robots.get("*").getCrawlDelay("*"));
					}
				}
				//Thread.sleep(robots.get(site).getCrawlDelay(site));
			} catch (InterruptedException e) {
				
				e.printStackTrace();
			}
    	return false;
    }
    
    @Override
    public boolean isOKtoCrawl(String site, int port, boolean isSecure) {
       //whether link is allowed
    	if (!robots.containsKey(site)) {
            try {
                System.out.println("Site: " + site);
                URL url;
                if(port == 80)
                {
                	url = new URL(isSecure ? "https://" : "http://" + site + "/robots.txt");
                }
                else
                {
                	url = new URL(isSecure ? "https://" : "http://" + site + ":" + port+ "/robots.txt");
                }
                	//URL url = new URL(isSecure ? "https://" : "http://" + site + ":" + port+ "/robots.txt");
                RobotsTxtInfo robot = new RobotsTxtInfo();
                
                /***HTTPS**/
                if (isSecure) {
                    HttpsURLConnection conn = (HttpsURLConnection)url.openConnection();
                    
                    conn.setRequestMethod("GET");
                    int responseCode = conn.getResponseCode();
            		System.out.println("GET Response Code :: " + responseCode);
            		if (responseCode == HttpURLConnection.HTTP_OK) { // success
            			BufferedReader in = new BufferedReader(new InputStreamReader(
            					conn.getInputStream()));
            			String inputLine;
            			//StringBuffer response = new StringBuffer();
            			String userAgent = null;
            			while ((inputLine = in.readLine()) != null) {
            				if(inputLine.startsWith("User-agent: "))
    	                	{
    	                		String[] arr =inputLine.split(" ");
    	                		userAgent = arr[1];
    	                	}
    	                	else
    	                	{
    	                		if(inputLine.startsWith("Crawl-delay: "))
    	                		{
    	                			String[] arr =inputLine.split(" ");
    	                			if(userAgent != null)
    	                    			robot.addCrawlDelay(userAgent, Integer.parseInt(arr[1]));
    	                		}
    	                		if(inputLine.startsWith("Disallow: "))
    	                		{
    	                			String[] arr =inputLine.split(" ");
    	                			if(userAgent != null)
    	                    			robot.addDisallowedLink(userAgent, arr[1]);
    	                		}
    	                	}
            				//response.append(inputLine);
            			}
            			in.close();
            		}
            			
                    conn.disconnect();
                } 
                
                /***HTTP**/
                else {
                	 HttpURLConnection conn = (HttpURLConnection)url.openConnection();
                     
                     conn.setRequestMethod("GET");
                     int responseCode = conn.getResponseCode();
                     System.out.println("GET Response Code :: " + responseCode);
                     
                     if (responseCode == HttpURLConnection.HTTP_OK) { // success
             			BufferedReader in = new BufferedReader(new InputStreamReader(
             					conn.getInputStream()));
             			String inputLine;
             			//StringBuffer response = new StringBuffer();
             			String userAgent = null;
             			while ((inputLine = in.readLine()) != null) {
             				System.out.println("userAgent: "+ userAgent +"Line from robots.txt :"+ inputLine);
             				if(inputLine.startsWith("User-agent: "))
     	                	{
     	                		String[] arr =inputLine.split(" ");
     	                		userAgent = arr[1];
     	                		//System.out.println("User-agent: " + userAgent);
     	                	}
     	                	else
     	                	{
     	                		if(inputLine.startsWith("Crawl-delay: "))
     	                		{
     	                			String[] arr =inputLine.split(" ");
     	                			
     	                			if(userAgent != null)
     	                    			robot.addCrawlDelay(userAgent, Integer.parseInt(arr[1]));
     	                			//System.out.println("crawlDelay " + Integer.parseInt(arr[1]));
     	                		}
     	                		if(inputLine.startsWith("Disallow: "))
     	                		{
     	                			String[] arr =inputLine.split(" ");
     	                			if(userAgent != null)
     	                    			robot.addDisallowedLink(userAgent, arr[1]);
     	                			System.out.println("Dissallowed: " + arr[1]);
     	                		}
     	                	}
             				//response.append(inputLine);
             			}
             			in.close();
             		}
             			
                     conn.disconnect();
                	
//                	String redirect;
                	
//                	if( (redirect= sendGetRobotsHttp(url.toString(),robot, site, port, false)) != null)
//                	{
////                		site = redirect.substring(redirect.find("http://")+7)
//                		sendGetRobotsHttp(redirect, robot, site, port, true);
//                		
//                	}
//                		
                	robots.put(site, robot);
                	
                }
                
            } catch (Exception e) {
                e.printStackTrace();
            }

        } 
        return robots.get(site) == null || (robots.get(site).getDisallowedLinks("*") == null ||
            !robots.get(site).getDisallowedLinks("*").contains("/")) ||
            (robots.get(site).getDisallowedLinks("cis455crawler") == null ||
            !robots.get(site).getDisallowedLinks("cis455crawler").contains("/"));
    }
    
   
    @Override
    public boolean isOKtoParse(URLInfo url) {
       //if html 
    	//Send HEAD request
    	url.size(info.size());
    	boolean return_val=false;
    	try {
	    	/***HTTPS****/
    		boolean downloadDocument = false;
    		if(url.isSecure())
	    	{
	    		URL siteUrl = new URL("https://" + url.getHostName() + ":" + url.getPortNo()+ "/" + url.getFilePath());
	    		HttpsURLConnection conn = (HttpsURLConnection)siteUrl.openConnection();
				conn.setRequestMethod("HEAD");
                int responseCode = conn.getResponseCode();
                if (responseCode == HttpURLConnection.HTTP_OK) { // success
                	String MIME = conn.getHeaderField("Content-Type");
                	if(MIME != null)
                	{
	                	if(MIME.equals("text/html") || MIME.endsWith("xml"))
	                		return_val = true;
                	}
                	String size = conn.getHeaderField("Content-Length");
                	if(Long.parseLong(size, 10) <= url.size())
                	{
                		downloadDocument = true;
                		return_val &= true;
                	}
                	
                }
                conn.disconnect();
                
	    	}
    		/***HTTP****/
	    	else
	    	{

	    		
	    		URL siteUrl = new URL("http://" + url.getHostName() + ":" + url.getPortNo()+ "/" + url.getFilePath());
	    		HttpURLConnection conn = (HttpURLConnection)siteUrl.openConnection();
                
                conn.setRequestMethod("HEAD");
                int responseCode = conn.getResponseCode();
                System.out.println("HEAD Response Code :: " + responseCode);
            	
                if (responseCode == HttpURLConnection.HTTP_OK) { // success
                	
                	String MIME = conn.getHeaderField("Content-Type");
                	if(MIME != null)
                	{
	                	if(MIME.equals("text/html"))
	                		return_val = true;
                	}
                	String size = conn.getHeaderField("Content-Length");
                	if(Long.parseLong(size, 10) <= url.size())
                	{
                		downloadDocument = true;
                		return_val &= true;
                		
                	}
                	
                }
                conn.disconnect();
                
//	    		Socket socket = new Socket(url.getHostName(),url.getPortNo()); 
//	    		PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))); 
//            	System.out.println("Sending request: "+"HEAD "+url/*+"/"+url.getFilePath()*/+" "+"HTTP/1.0");
	    		
//            	out.println("HEAD "+url/*+"/"+url.getFilePath()*/+" "+"HTTP/1.0"); 
//            	out.println(); 
//            	out.flush();               
//                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())); 
//
//            	String inputLine; 
//            	int count = 0; 
//            	while ((inputLine = in.readLine()) != null && !inputLine.equals("")) { 
//                	count++; 
//                	System.out.println("Line from header :"+ inputLine);
//                	if(inputLine.startsWith("Content-Type: "))
//                	{
//                		String[] arr = inputLine.split(" ");
//                		if((arr[1].equals("text/html")) || (arr[1].endsWith("xml")))
//                				return_val = true;
//                	}
//                	if(inputLine.startsWith("Content-Length: "))
//                	{
//                		String[] arr = inputLine.split(" ");
//                		if(Long.parseLong(arr[1], 10) <= url.size())
//                				return_val &= true;
//                	}
//            	}
            	
            	//Read Body
//            	while ((inputLine = in.readLine()) != null && !inputLine.equals("")) { 
//                	count++; 
//                	System.out.println("Line from body :"+ inputLine);
//            	}
//            	in.close();
//            	socket.close();
                	
	    	}
    		
    		if(downloadDocument)
        	{
        		downloadDocument(url);
        	}
    		
    	} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return return_val;
    }

    public void downloadDocument(URLInfo url)
    {
    	try {
    	//https
	    	if(url.isSecure())
	    	{
	    		URL siteUrl;
				
					siteUrl = new URL("https://" + url.getHostName() + ":" + url.getPortNo()+ "/" + url.getFilePath());
				
	    		HttpsURLConnection conn = (HttpsURLConnection)siteUrl.openConnection();
				conn.setRequestMethod("GET");
	            int responseCode = conn.getResponseCode();
	            if (responseCode == HttpURLConnection.HTTP_OK) { // success
	            	String length = conn.getHeaderField("Content-Length");
                	if(length != null)
                	{
//                		byte[] bytes;
//                		InputStreamReader is = new InputStreamReader(conn.getInputStream());
//                		int read = is.read(bytes, Integer.parseInt(length));

		            	BufferedReader in = new BufferedReader(new InputStreamReader(
	         					conn.getInputStream()));
		            	String files = null;
	         			String inputLine;
	         			while ((inputLine = in.readLine()) != null) {
	         				System.out.println("reading file line: "+inputLine);
	         				files += inputLine;
	         			}
	         			db.addDocument(url.getHostName()+"/"+url.getFilePath(), files);
	         			in.close();
	         			System.out.println("File stroed: "+ db.getDocument(url.getHostName()+"/"+url.getFilePath()));
                	}
	            }
	            conn.disconnect();
	    	}
	    	//http
	    	else
	    	{
	    		URL siteUrl;
				
				siteUrl = new URL("http://" + url.getHostName() + ":" + url.getPortNo()+ "/" + url.getFilePath());
			
    		HttpURLConnection conn = (HttpURLConnection)siteUrl.openConnection();
			conn.setRequestMethod("GET");
            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) { // success
            	String length = conn.getHeaderField("Content-Length");
            	if(length != null)
            	{
//            		byte[] bytes;
//            		InputStreamReader is = new InputStreamReader(conn.getInputStream());
//            		int read = is.read(bytes, Integer.parseInt(length));

	            	BufferedReader in = new BufferedReader(new InputStreamReader(
         					conn.getInputStream()));
	            	String files = null;
         			String inputLine;
         			while ((inputLine = in.readLine()) != null) {
         				System.out.println("reading file line: "+inputLine);
         				files += inputLine;
         			}
         			db.addDocument(url.getHostName()+"/"+url.getFilePath(), files);
         			in.close();
         			System.out.println("File stroed: "+ db.getDocument(url.getHostName()+"/"+url.getFilePath()));
            	}
            }
            conn.disconnect();
	    	}
    	} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
   
    
    @Override
    public void incCount() {
        System.out.print(".");
        crawled++;
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
    public synchronized void setWorking(boolean working) {
        if (working)
            busy++;
        else
            busy--;
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
        
        System.out.println("Crawler starting");
        String startUrl = args[0];
        String envPath = args[1];
        Integer size = Integer.valueOf(args[2]);
        Integer count = args.length == 4 ? Integer.valueOf(args[3]) : 100;
        
        if (!Files.exists(Paths.get(args[1]))) {
            try {
                Files.createDirectory(Paths.get(args[0]));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        StorageInterface db = StorageFactory.getDatabaseInstance(envPath);
        
        Crawler_MS1 crawler = new Crawler_MS1(startUrl, db, size, count);
        
        System.out.println("Starting crawl of " + count + " documents, starting at " + startUrl);
        
        XPathEngineImpl xpaths = (XPathEngineImpl) getXPathEngine();
        String[] xpathsArray = {"/a/b/c", "/xyz/abc[contains(text(),   \"something\")]", "/a/b/c[text()=\"EntireText\"]",
        		"/d/e/f/foo[text()=\"something\"]/bar", "/a/b/c/[text() = \"whiteSpace\"]", "/a/b/["};
        
        xpaths.setXPaths(xpathsArray);
        for(int i= 0; i< xpathsArray.length; i++)
        {
        	System.out.println(i +":"+xpaths.isValid(i));
        }
        crawler.start();
        
        while (!crawler.isDone())
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
        crawler.waitForThreadsToEnd();
        crawler.close();
        System.out.println("Done crawling!");
    }
    
//  private String sendGetRobotsHttp(String url, RobotsTxtInfo robot, String site, int port, boolean pre_redirect)
//  {
//  	
//		try {
//			Socket socket;
//			socket = new Socket(site,port);
//		
//
//  	PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))); 
//  	
//  	System.out.println("Connecting to "+url);
//  	
//  	out.print("GET " + "http://ec2-54-235-60-167.compute-1.amazonaws.com/robots.txt"+" HTTP/1.1\r\n"); 
////   	if(pre_redirect)
////   	{
//   		out.print("Host: "+ "ec2-54-235-60-167.compute-1.amazonaws.com\r\n");
////   	}
////   	else
////   	{
////   		out.println("Host: "+ site +"\r");
////   	}
//  	
//  	out.print("User-Agent: "+ "cis455crawler\r\n");
//  	//out.println("\r");
//  	out.println(); 
//  	out.flush(); 
//
//  	System.out.println("Sending: ");
//  	System.out.println("GET " + "http://ec2-54-235-60-167.compute-1.amazonaws.com/robots.txt"+" HTTP/1.1");
//  	System.out.println("Host: "+ "ec2-54-235-60-167.compute-1.amazonaws.com");
//  	System.out.println("User-Agent: "+ "cis455crawler");
//  	//out.println("\r");
//  	System.out.println(); 
//  	
//  	BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())); 
//
//  	String inputLine; 
//  	int count = 0; 
//  	String userAgent = null;
//  	boolean redirect = false;
//  	String redirectLink = null;
//  	//Reading Headers
//  	int contentLength = 0;
//  	while (((inputLine = in.readLine()) != null) && !inputLine.equals("")) { 
//      	count++; 
//      	//System.out.println(count); 
//      	System.out.println("Line from Robots.txt :"+ inputLine); 
//      	if(inputLine.startsWith("HTTP/1.1 "))
//      	{
//      		String arr[] = inputLine.split(" ");
//      		if(arr[1].equals("301"))
//      		{
//      			redirect = true;
//      			continue;
//      		}
//      	}
//      	if(inputLine.startsWith("Location: "))
//      	{
//      		String arr[] = inputLine.split(" ");
//      		
//      		redirectLink = arr[1];
//      		//System.out.println("Found REDIRECT");
//      		
//      		
//      	}
//      	if(inputLine.startsWith("User-agent: "))
//      	{
//      		String[] arr =inputLine.split(" ");
//      		userAgent = arr[1];
//      	}
//      	else
//      	{
//      		if(inputLine.startsWith("Crawl-delay: "))
//      		{
//      			String[] arr =inputLine.split(" ");
//      			if(userAgent != null)
//          			robot.addCrawlDelay(userAgent, Integer.parseInt(arr[1]));
//      		}
//      		if(inputLine.startsWith("Disallow: "))
//      		{
//      			String[] arr =inputLine.split(" ");
//      			if(userAgent != null)
//          			robot.addDisallowedLink(userAgent, arr[1]);
//      		}
//      	}
//  	} 
//  	
////  	if(!redirect)
////  	{
////	    	while(((inputLine = in.readLine()) != null) && !inputLine.equals("")) {
////	    		count++; 
//////	    		if(inputLine.startsWith("Content-Length: "))
//////          	{
//////          		String[] arr = inputLine.split(" ");
//////          		if(Long.parseLong(arr[1], 10) <= url.size())
//////          				return_val &= true;
//////          	}
////	    		//System.out.println(count); 
////	        	System.out.println("Body from Robots.txt :"+ inputLine);
////	    	}
////  	}
//     	in.close(); 
//  	socket.close();
//  	if(redirect)
//  	{
//  		return redirectLink;
//  	}
//  	
//  		
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} 
//		System.out.println("Returning from the function");
//		return null;
//  }

}
