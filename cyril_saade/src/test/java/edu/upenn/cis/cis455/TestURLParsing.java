package edu.upenn.cis.cis455;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.cis455.crawler.CrawlWorker;
import edu.upenn.cis.cis455.crawler.info.URLInfo;

public class TestURLParsing {
    
    //@Test
    public void testURLParsing() {

        CrawlWorker worker = new CrawlWorker(null, null, null);
        URLInfo info = new URLInfo("https://google.com/ok");
        String res1 = worker.findAbsoluteURL(info, "o2");
        String res2 = worker.findAbsoluteURL(info, "/ok");
        String res3 = worker.findAbsoluteURL(info, "../hello");
        System.out.println(res2);
        System.out.println(res1);
        System.out.println(res3);
        assert(res1.equals("https://google.com/ok/o2") && res2.equals("https://google.com/ok") && res3.equals("https://google.com/hello"));
    }
}
