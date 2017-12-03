package edu.upenn.cis.cis455.crawler.handlers;

import spark.Request;
import spark.Route;
import spark.Response;

import java.util.ArrayList;
import java.util.Set;

import edu.upenn.cis.cis455.storage.DBWrapper;
import edu.upenn.cis.cis455.storage.StorageInterface;
import spark.HaltException;


public class HomeScreen implements Route {
    
	StorageInterface db;
	
	public HomeScreen(StorageInterface db) {
    	this.db = db;  
    }

    @Override
    public String handle(Request req, Response resp) throws HaltException {
        StringBuilder ret = new StringBuilder();
        
        ret.append("<html><head><title>Welcome to CIS 455/555 HW2</title></head>");
        ret.append("<body><h1>Welcome to CIS 455/555 HW2</h1>");
        ret.append("Welcome, " + req.attribute("user"));
        
        
        //Set<String> channels = ((DBWrapper) db).getChannels();
//        if(channels != null) {
//        	for(String channel: channels)
//        	{
//        		if(channel != null)
//        		{
        			
        String username = req.session().attribute("user");
        ArrayList<String> channels = ((DBWrapper) db).getChannel(username);
        System.out.println("Username: "+username);
        
        if(channels!= null)
        	{
        	for(String channel:channels)
        	{
            	System.out.println("Printing channel");
            	String xPath = ((DBWrapper) db).getXpath(channel);
        		ret.append("<div class=\"channelheader\">Channel name: "+channel+",created by: "+username + " xPath :"+xPath);
            }
        }
        ret.append("<ul>");
        			
        
//        			
//        ret.append("<div class=\"channelheader\">Channel name: "+channel+",created by: "+username);
        
        
        			
//        		}
//        		
//        		
//        	}
        	
//        }
        
        
        ret.append("<li><a href='/login-form.html'>Log in as a different user</a>");
        ret.append("<li><a href='/register.html'>Register a user</a>");
        ret.append("<li><a href='/logout'>Log out</a>");
        ret.append("</ul>");
        
        ret.append("</body></html>");
        
        resp.type("text/html");
        return ret.toString();
    }
}
