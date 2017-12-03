package edu.upenn.cis.cis455.crawler.handlers;

import spark.Request;
import spark.Route;
import spark.Response;
import spark.HaltException;

import edu.upenn.cis.cis455.storage.StorageInterface;

public class CreateXPathHandler implements Route {
    StorageInterface db;
    
    public CreateXPathHandler(StorageInterface db) {
        this.db = db;    
    }

    @Override
    public String handle(Request req, Response resp) throws HaltException {
        String username = req.attribute("user");
        String channelName = req.params(":name");
        String xpath = req.queryParams("xpath");
        
        db.addXPath(channelName, xpath);
        db.addSubscription(username, channelName);
        
        return "";
    }
}
