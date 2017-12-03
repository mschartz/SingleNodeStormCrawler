package edu.upenn.cis.stormlite.bolt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.net.ssl.HttpsURLConnection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static edu.upenn.cis.cis455.crawler.Crawler.*;

import edu.upenn.cis.cis455.crawler.Crawler;
import edu.upenn.cis.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis.cis455.crawler.info.URLInfo;
import edu.upenn.cis.cis455.storage.DBWrapper;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.storage.StorageInterface;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;

public class DocumentFetcherBolt implements IRichBolt {
	static Logger log = LogManager.getLogger(DocumentFetcherBolt.class);

	String executorId = UUID.randomUUID().toString();
	Fields myFields = new Fields();
	
	OutputCollector collector;
	
	final StorageInterface db;
	boolean toParse;
	int docId;
	public DocumentFetcherBolt
	() { 
		System.out.println("creating doc fetcher bolt");
		this.db = StorageFactory.getDatabaseInstance(Crawler.envPath);
		this.toParse = false;
	}
	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("docId", "url", "toParse"));

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple input) {
//		System.out.println("Here in bolt");
		System.out.println("Doc Fetcher Bolt "+getExecutorId() + ": " + input.toString());
		String url =  input.getStringByField("url");
		
		URLInfo urlObj = new URLInfo(url);
		
		if(isOKtoCrawl(urlObj.getHostName(), urlObj.getPortNo(), urlObj.isSecure()))
		{
			//Check for disallowed
			boolean allowed = true;
			List<String> disLinks;
			if( (disLinks = robots.get(urlObj.getHostName()).getDisallowedLinks("cis455crawler")) != null)
			{
				//Check without addd /
				for( String dis: disLinks) {
					if (urlObj.getFilePath().equals(dis)) {
						allowed = false;
						break;
					}
				}
				//Check after addd /
				if( allowed == true)
				{
					for( String dis: disLinks) {
						String filepath = urlObj.getFilePath();
						if(!dis.endsWith("/"))
							filepath = urlObj.getFilePath() + "/";
						if (filepath.equals(dis)) {
							allowed = false;
							break;
						}
					}
				}
				//Check substring
//				if( allowed == true)
//				{
//					for( String dis: disLinks) {
//						String filepath = urlObj.getFilePath();
//						if(!dis.endsWith("/"))
//							filepath = urlObj.getFilePath() + "/";
//						if (dis.contains(filepath)) {
//							allowed = false;
//							break;
//						}
//					}
//				}
			}
			else if( (disLinks = robots.get(urlObj.getHostName()).getDisallowedLinks("*")) != null)
			{
				//Check without addd /
				for( String dis: disLinks) {
					if (urlObj.getFilePath().equals(dis)) {
						allowed = false;
						break;
					}
				}
				//Check after addd /
				if( allowed == true)
				{
					for( String dis: disLinks) {
						String filepath = urlObj.getFilePath();
						if(!dis.endsWith("/"))
							filepath = urlObj.getFilePath() + "/";
						if (filepath.equals(dis)) {
							allowed = false;
							break;
						}
					}
				}
				//Check substring
//				if( allowed == true)
//				{
//					for( String dis: disLinks) {
//						String filepath = urlObj.getFilePath();
//						if(!dis.endsWith("/"))
//							filepath = urlObj.getFilePath() + "/";
//						if (dis.contains(filepath)) {
//							allowed = false;
//							break;
//						}
//					}
//				}
			}
			
			if( allowed )
			{
				//DeferCrawl
				deferCrawl(urlObj.getHostName()); //Ignore the bool result thread sleeps
				
				//Download the doc --> add to content seen map by hashing the content
				boolean toEmit = isOKtoParse(urlObj);
				
			
				//Update the last modified time
				
				
				//emit docId, url, to_parse
				if(toEmit)
				{
					collector.emit(new Values<Object>(String.valueOf(this.docId), url, String.valueOf(this.toParse) ));
					System.out.println(getExecutorId() + " emitting from bolt");
				}
				//db.getDocument(urlObj.getHostName()+"/"+urlObj.getFilePath());
			}
			else
			{
				//Do nothing -- remove url ??
			}
		}
		
	}
	
	public boolean isOKtoCrawl(String site, int port, boolean isSecure) {
	       //whether link is allowed
		synchronized(robots)
		{
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
	             			}
	             			in.close();
	             		}
	             			
	                     conn.disconnect();
	                              		
	                	robots.put(site, robot);
	                	
	                }
	                
	            } catch (Exception e) {
	                e.printStackTrace();
	            }

	        } 
	    	robots.notifyAll();
		}
        return robots.get(site) == null || (robots.get(site).getDisallowedLinks("*") == null ||
            !robots.get(site).getDisallowedLinks("*").contains("/")) ||
            (robots.get(site).getDisallowedLinks("cis455crawler") == null ||
            !robots.get(site).getDisallowedLinks("cis455crawler").contains("/"));
	    }

	 
	public boolean deferCrawl(String site) {
	       //Check for crawl delay from robots.txt
	    	//TO DO: Change sleep to check time elapsed
	    synchronized(robots)
	    {
			if(robots.containsKey(site))
					try {
						if(robots.get(site).containsUserAgent("cis455crawler"))
						{
							int time = robots.get(site).getCrawlDelay(site);
							robots.notifyAll();
							Thread.sleep(time);
						}
						else
						{
							if(robots.get(site).containsUserAgent("*"))
							{
								int time = robots.get(site).getCrawlDelay("*");
								robots.notifyAll();
								Thread.sleep(time);
							}
						}
						//Thread.sleep(robots.get(site).getCrawlDelay(site));
					} catch (InterruptedException e) {
						
						e.printStackTrace();
					}
		    	return false;
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

	public boolean isOKtoParse(URLInfo url) {
	       //if html 
	    	//Send HEAD request
	    	url.size(Crawler.size);
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
		                		downloadDocument = true;
		                	if(MIME.equals("text/html"))
		                		this.toParse = true;
		                			
	                	}
	                	String size = conn.getHeaderField("Content-Length");
	                	if(size != null)
	                	{
		                	if(Long.parseLong(size, 10) <= url.size())
		                	{
		                		downloadDocument &= true;
		                		return_val = true;
		                	}
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
		                	if(MIME.equals("text/html")|| MIME.endsWith("xml"))
		                		downloadDocument = true;
		                	if(MIME.equals("text/html"))
		                		this.toParse = true;
	                	}
	                	String size = conn.getHeaderField("Content-Length");
	                	if(size != null)
	                	{
	                		
		                	if(Long.parseLong(size, 10) <= url.size())
		                	{
		                		downloadDocument &= true;
		                		return_val = true;
		                		
		                	}
	                	}
	                	
	                }
	                conn.disconnect();
	                
	                	
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
//	                		byte[] bytes;
//	                		InputStreamReader is = new InputStreamReader(conn.getInputStream());
//	                		int read = is.read(bytes, Integer.parseInt(length));

			            	BufferedReader in = new BufferedReader(new InputStreamReader(
		         					conn.getInputStream()));
			            	String files = null;
		         			String inputLine;
		         			while ((inputLine = in.readLine()) != null) {
		         				//System.out.println("reading file line: "+inputLine);
		         				if(files == null)
		         					files = inputLine;
		         				else
		         				{
		         					files += inputLine;
		         				}
		         			}
		         			in.close();
		         			//Check if content seen before adding to database
		         			if(!((DBWrapper) db).isContentSeen(files))
		         			{
			         			this.docId = db.addDocument(url.getHostName()+"/"+url.getFilePath(), files);
			         			
			         			((DBWrapper) db).addContentSeen(files);
		         			}
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

		            	BufferedReader in = new BufferedReader(new InputStreamReader(
	         					conn.getInputStream()));
		            	String files = null;
	         			String inputLine;
	         			while ((inputLine = in.readLine()) != null) {
	         				//System.out.println("reading file line: "+inputLine);
	         				if(files == null)
	         					files = inputLine;
	         				else
	         				{
	         				files += inputLine;
	         				}
	         			}
	         			in.close();
	         			//Check if content seen before adding to database
	         			if(!((DBWrapper) db).isContentSeen(files))
	         			{
	         				this.docId = db.addDocument(url.getHostName()+"/"+url.getFilePath(), files);
		         			
		         			((DBWrapper) db).addContentSeen(files);
	         			}
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
	    
	   
	    
	 

}
