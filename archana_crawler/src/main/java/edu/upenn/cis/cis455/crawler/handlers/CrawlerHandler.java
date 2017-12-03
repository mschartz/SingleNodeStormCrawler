package edu.upenn.cis.cis455.crawler.handlers;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.cis455.storage.DBWrapper;
import edu.upenn.cis.cis455.storage.StorageInterface;
import spark.Request;
import spark.Response;
import spark.Route;

public class CrawlerHandler  implements Route {

	final StorageInterface db;
	
	public CrawlerHandler(StorageInterface db) {
        this.db = db;
    }

	@Override
	public String handle(Request request, Response response) throws Exception {
		String name = request.queryParams("channel");
		
		StringBuilder ret = new StringBuilder();
		Date date = new Date();
		String username = request.session().attribute("user");
		ArrayList<String> channels = ((DBWrapper) db).getChannel(username);
		if(channels != null ) {
			boolean channelSeen = false;
			for(String channel : channels) {
				if(channel == null || !channel.equals(name)) continue;
				channelSeen = true;
				Map<String,String> urlDoc = ((DBWrapper) db).getMatchedDocs(channel);
				if(urlDoc!= null) {
					Set<String> urls = urlDoc.keySet();
					if(urls != null) {
						for(String url : urls) {
							
							ret.append("Crawled on: "+date.toString()+",Location: "+url);
							ret.append("<div class=\"document\">"+urlDoc.get(url));
						}
					}
				}
				else
				{
					ret.append("No matched documents");
				}
				
			}
			if(channelSeen == false)
				ret.append("Not subscribed to the channel "+ name);
		}
		else
		{
			ret.append("No subscribed channels");
		}
		
		
		
		response.type("text/html");
		return ret.toString();
	}
}