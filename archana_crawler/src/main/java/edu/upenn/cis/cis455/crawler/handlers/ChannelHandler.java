package edu.upenn.cis.cis455.crawler.handlers;

import spark.Request;
import spark.Response;
import spark.Route;

import java.util.Set;

import edu.upenn.cis.cis455.storage.DBWrapper;
import edu.upenn.cis.cis455.storage.StorageInterface;

public class ChannelHandler implements Route {

	final StorageInterface db;
	
	public ChannelHandler(StorageInterface db) {
        this.db = db;
    }
	@Override
	public String handle(Request request, Response response) throws Exception {
		
		System.out.println("Handler");
		String fullpath = request.pathInfo();
		String name = null;
		if(fullpath.contains("/"))
		{
			String[] names = fullpath.split("/");
			name = names[names.length-1];
			if(names.length != 3) 		//Needs to be /create/name-of-channel
			{
				return "Bad Request";
			}
		}
		else
		{
			return "Bad Request";
		}
		String xpath = request.queryParams("xpath");
		//System.out.println(name+": "+xpath);
		String username = request.session().attribute("user");
		String ret = ((DBWrapper) db).addChannel(username,name,xpath);
		if(ret == null)
		{
			return "Channel name already exists. Use a different name"; 
		}
		String new_xpath = ((DBWrapper) db).getXpath(name);
		System.out.println("Added "+name+" "+xpath);
		return "Channel added successfully!";
	}

}
