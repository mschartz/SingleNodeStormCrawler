package edu.upenn.cis.cis455;

import edu.upenn.cis.cis455.crawler.info.URLInfo;
import org.junit.*;
import org.junit.Test;
import org.junit.Assert.*;
import junit.framework.TestCase;
import edu.upenn.cis.cis455.crawler.HttpClient;
public class CrawlerTests extends TestCase {

    @Test
    public void test() {
        String hello = "hello";
        assert (hello.equals("hello"));
        HttpClientTest();
    }

    public void seedPageHeadTest(){
        assert (1==1);
    }

    public void correctPath(){
        URLInfo u = new URLInfo("https://google.com");
        assert (u.getFilePath() == "/");

        URLInfo nf = new URLInfo("nickfausti.com/projects");
        assert (nf.getFilePath() == "/projects");

    }

    public void HttpClientTest(){
        System.out.println("in httpclienttest");
        URLInfo nf = new URLInfo("nickfausti.com/projects");
        HttpClient client = new HttpClient();
        String res = client.sendRequest(nf, "GET");
        System.out.println(res);
    }
}
