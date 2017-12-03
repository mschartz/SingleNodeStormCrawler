package edu.upenn.cis.cis455.crawler.handlers;

import spark.Request;
import spark.Route;
import spark.Response;
import spark.HaltException;
import java.security.MessageDigest;

import static spark.Spark.halt;

import edu.upenn.cis.cis455.storage.StorageInterface;

public class RegistrationHandler implements Route {
    StorageInterface db;
    
    public RegistrationHandler(StorageInterface db) {
        this.db = db;    
    }

    @Override
    public String handle(Request req, Response resp) throws HaltException {
        try {
            if (req.queryParams().contains("username") && req.queryParams().contains("password") && req.queryParams().contains("firstname") && req.queryParams().contains("lastname")) {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                String pass = new String(md.digest(req.queryParams("password").getBytes()), "UTF-8");
    
    
                if (db.getSessionForUser(req.queryParams("username"), pass)) {
                    return "User already exists";
                } else {
                    System.err.println("Adding " + req.queryParams("username") + "/" +
                        pass);
                    System.out.println("Firstname is:" + req.queryParams("firstname"));
                    db.addUser(req.queryParams("firstname"), req.queryParams("lastname"), req.queryParams("username"), pass);
                    resp.redirect("/login-form");
                }
                
                
            } else
                halt(400, "Invalid form");
        }
        catch(Exception e) {
                halt(500, "Server Error");
        }

        
        return "";
    }
}
