package edu.upenn.cis.cis455;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.cis455.crawler.Crawler;
import edu.upenn.cis.cis455.crawler.info.URLInfo;

public class TestGetRobot {
    
    //@Test
    public void testGetRobot() {
        Crawler crawler = new Crawler("https://dbappserv.cis.upenn.edu/crawltest.html", null, 10, 10);
        URLInfo infoHttps = new URLInfo("https://dbappserv.cis.upenn.edu/");
        URLInfo infoHttp = new URLInfo("http://ec2-54-235-60-167.compute-1.amazonaws.com/crawltest/");
        try {
            assert(crawler.readRobotsFile(infoHttps) && crawler.readRobotsFile(infoHttp));
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }
}
