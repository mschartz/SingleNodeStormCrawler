package edu.upenn.cis.cis455.crawler.handlers;

import spark.Request;
import spark.Route;
import spark.Response;
import spark.HaltException;

import edu.upenn.cis.cis455.storage.StorageInterface;
import java.util.List;

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
        ret.append("Welcome, " + req.attribute("fullname"));
        
        ret.append("<ul>");
        ret.append("<li><a href='/login-form.html'>Log in as a different user</a>");
        ret.append("<li><a href='/register.html'>Register a user</a>");
        ret.append("<li><a href='/logout'>Log out</a>");
        ret.append("</ul>");
        
        ret.append("<h2>All Channels</h2>");
        ret.append("<ul>");
        for(String channelName : db.getAllXPaths().keySet()) {
            ret.append("<li>" + channelName + "</li>");
        }
        ret.append("</ul");
        
        ret.append("<h2>Subscribed Channels</h2>");
        ret.append("<ul>");
        
        db.getSubscriptions("csaade");
        
        if(db.getSubscriptions(req.attribute("user")) == null) {
            ret.append("<li> You have no subscriptions yet </li>");
        }
        else {
            List<String> subs;
            subs = db.getSubscriptions(req.attribute("user")).getSubscriptions();
            for(int i=0; i<subs.size(); i++) {
                ret.append("<li><a href='/show?channel=" + subs.get(i) +"'>" + subs.get(i) + "</a></li>");
            }
        }
        ret.append("</ul>");
        
        ret.append("</body></html>");
        
        resp.type("text/html");
        return ret.toString();
    }
}
