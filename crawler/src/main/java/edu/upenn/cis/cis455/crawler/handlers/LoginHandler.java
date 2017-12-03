package edu.upenn.cis.cis455.crawler.handlers;

import spark.Request;
import spark.Route;
import spark.Response;
import spark.HaltException;
import spark.Session;
import edu.upenn.cis.cis455.storage.StorageInterface;

import javax.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class LoginHandler implements Route {
    StorageInterface db;
    
    public LoginHandler(StorageInterface db) {
        this.db = db;
    }

    @Override
    public String handle(Request req, Response resp) throws HaltException {
        String user = req.queryParams("username");
        String pass = hash(req.queryParams("password"));
        
        System.err.println("Login request for " + user + " and " + pass);
        if (db.getSessionForUser(user, pass)) {
            System.err.println("Logged in!");
            Session session = req.session();
            
            session.attribute("user", user);
            session.attribute("password", pass);
            resp.redirect("/index.html");
        } else {
            System.err.println("Invalid credentials");
            resp.redirect("/login-form.html");
        }

            
        return "";
    }

    public String hash(String s){
        byte[] result = {};
        String strHash = "hash";
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            result = sha.digest(s.getBytes(StandardCharsets.UTF_8));
            strHash = DatatypeConverter.printHexBinary(result);
            System.out.println(strHash);
        }
        catch(java.security.NoSuchAlgorithmException e) {
            System.out.println("no such hash algorithmn");
        }
        return strHash;
    }

}
