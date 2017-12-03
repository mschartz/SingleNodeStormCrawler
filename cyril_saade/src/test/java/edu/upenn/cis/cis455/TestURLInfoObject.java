package edu.upenn.cis.cis455;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.cis455.crawler.info.URLInfo;

public class TestURLInfoObject {
    
    //@Test
    public void testURLObject() {
        URLInfo url = new URLInfo("http://www.google.com:8080/ok");
        
        assert(url.getFilePath().equals("/ok") && 
        url.getHostName().equals("google.com") && 
        (url.getPortNo() == 8080) && 
        url.getNextOperation().equals("Robot.txt"));
        
    }
    
}
