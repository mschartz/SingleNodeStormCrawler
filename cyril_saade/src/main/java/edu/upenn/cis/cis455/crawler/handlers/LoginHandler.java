package edu.upenn.cis.cis455.crawler.handlers;

import spark.Request;
import spark.Route;
import spark.Response;
import spark.HaltException;
import spark.Session;
import static spark.Spark.halt;
import edu.upenn.cis.cis455.storage.StorageInterface;

import java.security.MessageDigest;

public class LoginHandler implements Route {
    StorageInterface db;
    
    public LoginHandler(StorageInterface db) {
        this.db = db;
    }

    @Override
    public String handle(Request req, Response resp) throws HaltException {
        try {
            String user = req.queryParams("username");
            
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            String pass = new String(md.digest(req.queryParams("password").getBytes()), "UTF-8");
            
            
            System.err.println("Login request for " + user + " and " + pass);
            if (db.getSessionForUser(user, pass)) {
                System.err.println("Logged in!");
                Session session = req.session(); // TODO: create session that is going to expire in 5min
                session.maxInactiveInterval(300);
                session.attribute("user", user);
                session.attribute("fullname", db.getUserFullName(user, pass));
                session.attribute("password", pass);
                resp.redirect("/index.html");
            } else {
                System.err.println("Invalid credentials");
                resp.redirect("/login-form.html");
            }
        }
        catch(Exception e) {
            halt(500, "Server Error");
        }


            
        return "";
    }
}
