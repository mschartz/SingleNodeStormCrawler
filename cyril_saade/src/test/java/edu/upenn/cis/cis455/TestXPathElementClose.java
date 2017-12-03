package edu.upenn.cis.cis455;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.cis455.xpathengine.XPathFSM;
import edu.upenn.cis.cis455.model.OccurrenceEvent;

public class TestXPathElementClose {
    
    @Test
    public void testXPathClosing() {
        
        boolean isMatch = false;
        
        XPathFSM xpathA = new XPathFSM("/a/b/c");
        isMatch = xpathA.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementOpen, "a", 0));
        assert(!isMatch);
        isMatch = xpathA.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementClose, "a", 0));
        assert(!isMatch);
        isMatch = xpathA.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementOpen, "a", 0));
        assert(!isMatch);
        isMatch = xpathA.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementOpen, "b", 1));
        assert(!isMatch);
        isMatch = xpathA.transition(new OccurrenceEvent("doc1", OccurrenceEvent.EventType.ElementOpen, "c", 2));
        assert(isMatch);
        

        isMatch = xpathA.transition(new OccurrenceEvent("doc2", OccurrenceEvent.EventType.ElementOpen, "a", 0));
        assert(!isMatch);
        isMatch = xpathA.transition(new OccurrenceEvent("doc2", OccurrenceEvent.EventType.ElementOpen, "b", 1));
        assert(!isMatch);
        isMatch = xpathA.transition(new OccurrenceEvent("doc2", OccurrenceEvent.EventType.ElementClose, "b", 1));
        assert(!isMatch);
        isMatch = xpathA.transition(new OccurrenceEvent("doc2", OccurrenceEvent.EventType.ElementOpen, "b", 1));
        assert(!isMatch);
        isMatch = xpathA.transition(new OccurrenceEvent("doc2", OccurrenceEvent.EventType.ElementOpen, "c", 2));
        assert(isMatch);
        
    }
    
}
