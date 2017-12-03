package edu.upenn.cis.cis455.crawler.handlers;

import spark.Request;
import spark.Route;
import spark.Response;
import spark.HaltException;

import edu.upenn.cis.cis455.storage.StorageInterface;
import java.util.List;

import java.text.SimpleDateFormat;

public class ShowMatchedDocumentsHandler implements Route {
    StorageInterface db;
    
    public ShowMatchedDocumentsHandler(StorageInterface db) {
        this.db = db;
    }
    
    @Override
    public String handle(Request req, Response resp) throws HaltException {
        StringBuilder ret = new StringBuilder();
        ret.append("<html><head><title>Welcome to CIS 455/555 HW2</title></head>");
        ret.append("<body><h1>Welcome to CIS 455/555 HW2</h1>");
        ret.append("Welcome, " + req.attribute("fullname"));
        
        String channelName = req.queryParams("channel");
        
        List<String> docIDs = db.getMatches(channelName);
        if(docIDs != null) {
            for(int i=0; i<docIDs.size(); i++) {
                ret.append("<div class='channelheader'>");
                ret.append("Channel Name: " + channelName + "\n");
                ret.append("created by: " + req.attribute("user") + "\n"); // TODO: change TODO: only for XML 
                
                SimpleDateFormat dateBegin = new SimpleDateFormat("yyyy-MM-d");
                SimpleDateFormat dateEnd = new SimpleDateFormat("HH:mm:ss");
                String dateStrBegin = dateBegin.format(db.getDocumentTimeStamp(docIDs.get(i)));
                String dateStrEnd = dateEnd.format(db.getDocumentTimeStamp(docIDs.get(i)));
                
                ret.append("Crawled on: " + dateStrBegin + "T" + dateStrEnd + "\n");
                ret.append("Location: " + docIDs.get(i) + "\n"); // TODO: only for XML
                ret.append("</div>");
                
                ret.append("<div class='document'>");
                ret.append(db.getDocument(docIDs.get(i)));
                ret.append("</div>");
                ret.append("</div>");
                
            }
        }
        else {
            ret.append("No matches found for this channel. Try to crawl again!");
        }

        
        ret.append("</body></html>");
        
        resp.type("text/html");
        return ret.toString();
    }
}
