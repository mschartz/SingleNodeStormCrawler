package edu.upenn.cis.cis455;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.cis455.xpathengine.XPathFSM;
import edu.upenn.cis.cis455.model.OccurrenceEvent;

public class TestXPathMatching {
    
    @Test
    public void testXPathMatching() {
        
        boolean isMatch = false;
        
        XPathFSM xpathEasy = new XPathFSM("/a/b/c");
        isMatch = xpathEasy.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementOpen, "a", 0));
        assert(!isMatch);
        isMatch = xpathEasy.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementOpen, "z", 1));
        assert(!isMatch);
        isMatch = xpathEasy.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementClose, "z", 1));
        assert(!isMatch);
        isMatch = xpathEasy.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementOpen, "b", 1));
        assert(!isMatch);
        isMatch = xpathEasy.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementOpen, "c", 2));
        assert(isMatch);
        
        XPathFSM xpathMediumA = new XPathFSM("/a/b[text()=\"medium\"]");
        isMatch = xpathMediumA.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementOpen, "a", 0));
        assert(!isMatch);
        isMatch = xpathMediumA.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementOpen, "b", 1));
        assert(!isMatch);
        isMatch = xpathMediumA.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.Text, "medium", 2));
        assert(isMatch);
        
        XPathFSM xpathMediumB = new XPathFSM("/a/b[contains(text(), \"hello\")]");
        isMatch = xpathMediumB.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementOpen, "a", 0));
        assert(!isMatch);
        isMatch = xpathMediumB.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementOpen, "b", 1));
        assert(!isMatch);
        isMatch = xpathMediumB.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.Text, "hello there", 2));
        assert(isMatch);
        
        XPathFSM xpathHard = new XPathFSM("/foo/bar/zyz[text()= \"hello\"]/done");
        isMatch = xpathHard.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementOpen, "foo", 0));
        assert(!isMatch);
        isMatch = xpathHard.transition(new OccurrenceEvent("doc2", OccurrenceEvent.EventType.ElementOpen, "bar", 0));
        assert(!isMatch);
        isMatch = xpathHard.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementOpen, "bar", 1));
        assert(!isMatch);
        isMatch = xpathHard.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementOpen, "zyz", 2));
        assert(!isMatch);
        isMatch = xpathHard.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.Text, "hello", 3));
        assert(!isMatch);
        isMatch = xpathHard.transition(new OccurrenceEvent("doc2", OccurrenceEvent.EventType.ElementOpen, "done", 1));
        assert(!isMatch);
        isMatch = xpathHard.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementOpen, "done", 4));
        assert(isMatch);
    }
    
}
