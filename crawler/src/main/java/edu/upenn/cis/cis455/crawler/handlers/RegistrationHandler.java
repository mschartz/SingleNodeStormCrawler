package edu.upenn.cis.cis455.crawler.handlers;

import spark.Request;
import spark.Route;
import spark.Response;
import spark.HaltException;

import static spark.Spark.halt;

import edu.upenn.cis.cis455.storage.StorageInterface;

import javax.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;


public class RegistrationHandler implements Route {
    StorageInterface db;
    
    public RegistrationHandler(StorageInterface db) {
        this.db = db;    
    }

    @Override
    public String handle(Request req, Response resp) throws HaltException {
        if (req.queryParams().contains("username") && req.queryParams().contains("password")) {
            if (db.getSessionForUser(req.queryParams("username"), hash(req.queryParams("password")))) {
                return "User already exists";
            } else {
                System.out.println("Adding " + req.queryParams("username") + "/" +
                    req.queryParams("password"));
                db.addUser(req.queryParams("username"), hash(req.queryParams("password")));
                resp.redirect("/login-form");
            }
            
            
        } else
            halt(400, "Invalid form");
        
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
