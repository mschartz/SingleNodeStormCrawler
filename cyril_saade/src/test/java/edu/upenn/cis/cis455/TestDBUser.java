package edu.upenn.cis.cis455;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.storage.StorageInterface;

public class TestDBUser {
    
    //@Test
    public void testDBUser() {
        try {
            StorageInterface db = StorageFactory.getDatabaseInstance("./databaseTest");
            db.addUser("Cyril", "Saade", "csaade", "abc");
            
            assert(db.getUserFullName("csaade", "abc").equals("Cyril Saade"));
            db.close();
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }
}
