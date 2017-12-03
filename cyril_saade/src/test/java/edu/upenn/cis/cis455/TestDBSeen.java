package edu.upenn.cis.cis455;

//import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;


import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.storage.StorageInterface;

//import java.io.ByteArrayInputStream;
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.net.InetAddress;
//import java.net.InetSocketAddress;
//import java.net.ServerSocket;
//import java.net.Socket;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;  
import java.security.MessageDigest;

public class TestDBSeen {
    
    //@Test
    public void testDBSeen() {
        try {
            StorageInterface db = StorageFactory.getDatabaseInstance("./database");
            
            String docBody = "xyz";
            MessageDigest md = MessageDigest.getInstance("MD5");
            String hash = new String(md.digest(docBody.getBytes("UTF-8")), "UTF-8");
            db.insertSeen(hash);
            assert db.hasSeen(hash);
            db.close();
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }
}
