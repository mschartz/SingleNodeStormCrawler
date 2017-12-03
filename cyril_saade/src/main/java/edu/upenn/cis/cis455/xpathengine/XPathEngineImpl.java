package edu.upenn.cis.cis455.xpathengine;

import edu.upenn.cis.cis455.xpathengine.XPathEngine;
import edu.upenn.cis.cis455.xpathengine.XPathFSM;
import edu.upenn.cis.cis455.model.OccurrenceEvent;

import java.util.List;
import java.util.ArrayList;

public class XPathEngineImpl implements XPathEngine {
    
    List<XPathFSM> xpaths;
    public XPathEngineImpl() {
        xpaths = new ArrayList<XPathFSM>();
    }

    /**
	 * Sets the XPath expression(s) that are to be evaluated.
	 * @param expressions
	 */
	public void setXPaths(String[] expressions) {
	    for(int i=0; i<expressions.length; i++) {
	        if(isValid(expressions[i])) {
	            xpaths.add(new XPathFSM(expressions[i]));
	        }
	        // TODO: construct XPathFSM(expression) -> add object to Xpath Queue
	    }
	}

	/**
	 * Returns true if the i.th XPath expression given to the last setXPaths() call
	 * was valid, and false otherwise. If setXPaths() has not yet been called, the
	 * return value is undefined. 
	 * @param i
	 * @return
	 */
	public boolean isValid(String currXpath) { // TODO: not sure what to do here
	   String []currXpathStr = currXpath.split("/");
	   if(currXpathStr[0].length() != 0) { // path is supposed to be absolute
	       return false;
	   }
	   
	   for(int j=1; j< currXpathStr.length; j++) {
	       String nodeName = "";
            int strIndex;
            for(strIndex=0; strIndex<currXpathStr[j].length(); strIndex++) {
                if(currXpathStr[j].charAt(strIndex) == '[')
                    break;
                nodeName += currXpathStr[j].charAt(strIndex);
            }
            
            if(nodeName.equals("")) { // "//" is not allowed
                return false;
            }
            strIndex++;
            
            if(strIndex >= currXpathStr[j].length() -1) { // xpath doesnt contain a [
                continue;
            }
            
            String command = "";
            while(currXpathStr[j].charAt(strIndex) != '(') { // check if there is a (
                command+=currXpathStr[j].charAt(strIndex);
                if(strIndex >= currXpathStr[j].length() -1) {
                    return false;
                }
                strIndex++;
            }
            strIndex++;

            if(strIndex >= currXpathStr[j].length()-1)
            	return false;
            System.out.println(command);
            if(command.trim().toLowerCase().equals("contains")) {
            	boolean first = true;
	            while(currXpathStr[j].charAt(strIndex) != ')' || first) { // check if there is a )
	            	if(strIndex >= currXpathStr[j].length() -1) {
	                    return false;
	                }
	                if(currXpathStr[j].charAt(strIndex) == ')')
	            		first = false;
	                strIndex++;
	            }
	        }
	        else {
	        	while(currXpathStr[j].charAt(strIndex) != '=') { // check if there is a )
	            	if(strIndex >= currXpathStr[j].length() -1) {
	                    return false;
	                }
	                strIndex++;
	            }
	        }    
            while(currXpathStr[j].charAt(strIndex) != ']') { // check if there is a ]
                if(strIndex >= currXpathStr[j].length() -1) {
                    return false;
                }
                strIndex++;
            }
	   }
	    return true;
	}
	
	/**
	 * Event driven pattern match.
	 * 
	 * Takes an event at a time as input
	 *
	 * @param event notification about something parsed, from a
	 * given document
	 * 
 	 * @return bit vector of matches to XPaths
	 */
	public boolean[] evaluateEvent(OccurrenceEvent event) {
	    
	    boolean[] matches = new boolean[xpaths.size()];
	    for(int i=0; i<xpaths.size(); i++) {
	        matches[i] = xpaths.get(i).transition(event);
	    }
	    
	    return matches;
	}
}
