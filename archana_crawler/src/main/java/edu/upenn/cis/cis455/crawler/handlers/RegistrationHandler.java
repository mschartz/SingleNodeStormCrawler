package edu.upenn.cis.cis455.crawler.handlers;

import spark.Request;
import spark.Route;
import spark.Response;
import spark.HaltException;

import static spark.Spark.halt;

import edu.upenn.cis.cis455.storage.DBWrapper;
import edu.upenn.cis.cis455.storage.StorageInterface;

public class RegistrationHandler implements Route {
    StorageInterface db;
    
    public RegistrationHandler(StorageInterface db) {
        this.db = db;    
    }

    @Override
    public String handle(Request req, Response resp) throws HaltException {
        if (req.queryParams().contains("username") && req.queryParams().contains("password")) {
            if (db.getSessionForUser(req.queryParams("username"), req.queryParams("password"))) {
                return "User already exists";
            } else {
                System.err.println("Adding " + req.queryParams("username") + "/" +
                    req.queryParams("password") + req.queryParams("firstname") + " " + req.queryParams("lastname") );
                int ret = db.addUser(req.queryParams("username"), req.queryParams("password"));
                if(!req.queryParams("firstname").isEmpty() && !req.queryParams("firstname").isEmpty())
                {
                	((DBWrapper) db).addNames(req.queryParams("username"), req.queryParams("firstname"), req.queryParams("lastname"));
                }
                if( ret == 0)
                {
                	System.err.println("Username already exists");
                	resp.body("Username already exists");             			
                }
                resp.redirect("/login-form");
            }
            
        } else
            halt(400, "Invalid form");
        
        return "";
    }
}
