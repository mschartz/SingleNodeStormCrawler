package edu.upenn.cis.cis455.crawler.handlers;

import edu.upenn.cis.cis455.crawler.Crawler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Request;
import spark.Response;
import spark.Route;
import edu.upenn.cis.cis455.storage.StorageInterface;

public class LookupHandler implements Route {
    final static Logger logger = LogManager.getLogger(LookupHandler.class);

    final StorageInterface db;
    
    public LookupHandler(StorageInterface db) {
        this.db = db;
    }

    @Override
    public String handle(Request req, Response resp) throws Exception {
        resp.type("text/html");
        //logger.debug(req.queryParams("url"));
        //db.getCorpusSize();
        String doc;
        try {
            doc = db.getDocument(req.queryParams("url"));
        }
        catch(IllegalArgumentException e) {
            doc = "here be dragons (no document found with that url)";
        }
  //      logger.debug("doc");
        return doc;
    }
    
    
}
