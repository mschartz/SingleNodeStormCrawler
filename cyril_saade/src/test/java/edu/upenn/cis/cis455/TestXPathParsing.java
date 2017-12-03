package edu.upenn.cis.cis455;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.cis455.xpathengine.XPathFSM;

public class TestXPathParsing {
    
    @Test
    public void testXPathParsing() {
        
        System.out.println("~~~~~~~~~~~~~~~");
        XPathFSM xpath1 = new XPathFSM("/foo/bar/xyz");
        System.out.println("~~~~~~~~~~~~~~~");
        XPathFSM xpath2 = new XPathFSM("/xyz/abc[contains(text(),\"someSubstring\")]");
        System.out.println("~~~~~~~~~~~~~~~");
        XPathFSM xpath3 = new XPathFSM("/a/b/c[text()=\"theEntireText\"]");
        System.out.println("~~~~~~~~~~~~~~~");
        XPathFSM xpath4 = new XPathFSM("/d/e/f/foo[text()=\"something\"]/bar");
        System.out.println("~~~~~~~~~~~~~~~");
        XPathFSM xpath5 = new XPathFSM("/a/b/c[text() =   \"whiteSpacesShouldNotMatter\"]");
    }
}
