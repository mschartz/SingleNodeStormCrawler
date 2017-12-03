package edu.upenn.cis.cis455;

import org.junit.Test;
import junit.framework.TestCase;

public class DatabaseTest extends TestCase {
    public DatabaseTest() {}

    @Test
    public void test() {
        String hello = "hello";
        System.out.println("in database test");
        assert (hello.equals("hello"));
    }
}
